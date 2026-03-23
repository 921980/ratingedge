import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from db import get_conn

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/portfolio", tags=["portfolio"])

MOVEHUT_ENDPOINT = "https://movehut.co.uk/wp-json/movehut/v1/create-from-ratingedge"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class MovehutPayload(BaseModel):
    property_id: int
    uprn: str | None = None
    address: str
    postcode: str
    epc_rating: str | None = None
    mitigation_score: float | None = None
    extra: dict[str, Any] | None = None


class MovehutResponse(BaseModel):
    success: bool
    movehut_id: str | None = None
    message: str | None = None


class PortfolioSummaryItem(BaseModel):
    property_id: int
    uprn: str | None
    address: str
    postcode: str
    epc_rating: str | None
    current_rating: str | None
    potential_rating: str | None
    mitigation_score: float | None
    estimated_saving_kwh: float | None
    estimated_saving_gbp: float | None
    opportunity_count: int


# ---------------------------------------------------------------------------
# Movehut integration helper
# ---------------------------------------------------------------------------

async def post_to_movehut(payload: MovehutPayload) -> MovehutResponse:
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(
                MOVEHUT_ENDPOINT,
                json=payload.model_dump(exclude_none=True),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return MovehutResponse(
                success=True,
                movehut_id=data.get("id") or data.get("movehut_id"),
                message=data.get("message"),
            )
        except httpx.HTTPStatusError as exc:
            logger.error(
                "Movehut returned %s for property %s: %s",
                exc.response.status_code,
                payload.property_id,
                exc.response.text,
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=(
                    "Movehut integration error: "
                    + str(exc.response.status_code)
                    + " "
                    + exc.response.text
                ),
            )
        except httpx.RequestError as exc:
            logger.error("Movehut request failed: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Movehut integration unreachable: " + str(exc),
            )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/summary",
    response_model=list[PortfolioSummaryItem],
    summary="Return portfolio summary joined to mitigation opportunities",
)
async def portfolio_summary(
    limit: int = 100,
    offset: int = 0,
    conn=Depends(get_conn),
) -> list[PortfolioSummaryItem]:
    sql = (
        "SELECT"
        "    p.property_id,"
        "    p.uprn,"
        "    p.address,"
        "    p.postcode,"
        "    p.epc_rating,"
        "    m.current_rating,"
        "    m.potential_rating,"
        "    m.mitigation_score,"
        "    m.estimated_saving_kwh,"
        "    m.estimated_saving_gbp,"
        "    COALESCE(m.opportunity_count, 0) AS opportunity_count"
        " FROM er_portfolio p"
        " LEFT JOIN vw_mitigation_opportunities m"
        "    ON m.property_id = p.property_id"
        " ORDER BY p.property_id"
        " LIMIT $1 OFFSET $2"
    )

    try:
        rows = await conn.fetch(sql, limit, offset)
    except Exception as exc:
        logger.error("portfolio_summary query failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error: " + str(exc),
        )

    return [PortfolioSummaryItem(**dict(row)) for row in rows]


@router.get(
    "/{property_id}",
    response_model=PortfolioSummaryItem,
    summary="Return a single portfolio property with mitigation data",
)
async def get_portfolio_property(
    property_id: int,
    conn=Depends(get_conn),
) -> PortfolioSummaryItem:
    sql = (
        "SELECT"
        "    p.property_id,"
        "    p.uprn,"
        "    p.address,"
        "    p.postcode,"
        "    p.epc_rating,"
        "    m.current_rating,"
        "    m.potential_rating,"
        "    m.mitigation_score,"
        "    m.estimated_saving_kwh,"
        "    m.estimated_saving_gbp,"
        "    COALESCE(m.opportunity_count, 0) AS opportunity_count"
        " FROM er_portfolio p"
        " LEFT JOIN vw_mitigation_opportunities m"
        "    ON m.property_id = p.property_id"
        " WHERE p.property_id = $1"
    )

    try:
        row = await conn.fetchrow(sql, property_id)
    except Exception as exc:
        logger.error("get_portfolio_property query failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error: " + str(exc),
        )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Property " + str(property_id) + " not found in portfolio",
        )

    return PortfolioSummaryItem(**dict(row))


@router.post(
    "/{property_id}/movehut",
    response_model=MovehutResponse,
    summary="Push a portfolio property to Movehut",
    status_code=status.HTTP_200_OK,
)
async def push_to_movehut(
    property_id: int,
    conn=Depends(get_conn),
) -> MovehutResponse:
    sql = (
        "SELECT"
        "    p.property_id,"
        "    p.uprn,"
        "    p.address,"
        "    p.postcode,"
        "    p.epc_rating,"
        "    m.mitigation_score"
        " FROM er_portfolio p"
        " LEFT JOIN vw_mitigation_opportunities m"
        "    ON m.property_id = p.property_id"
        " WHERE p.property_id = $1"
    )

    try:
        row = await conn.fetchrow(sql, property_id)
    except Exception as exc:
        logger.error("push_to_movehut query failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error: " + str(exc),
        )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Property " + str(property_id) + " not found in portfolio",
        )

    payload = MovehutPayload(
        property_id=row["property_id"],
        uprn=row["uprn"],
        address=row["address"],
        postcode=row["postcode"],
        epc_rating=row["epc_rating"],
        mitigation_score=row["mitigation_score"],
    )

    return await post_to_movehut(payload)
