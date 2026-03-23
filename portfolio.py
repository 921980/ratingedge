from fastapi import APIRouter, Query
from typing import Optional
import requests
from db import get_conn

router = APIRouter()

MOVEHUT_API_URL = “https://movehut.co.uk/wp-json/movehut/v1/create-from-ratingedge”

def create_movehut_listing(uprn: str, address: str) -> dict:
try:
response = requests.post(
MOVEHUT_API_URL,
json={“uprn”: uprn, “address”: address},
timeout=10
)
return response.json()
except Exception as e:
return {“error”: str(e)}

@router.get(”/portfolio/test-movehut”)
def test_movehut():
return create_movehut_listing(“TEST456”, “FastAPI Test Property”)

@router.post(”/portfolio/push-to-movehut”)
def push_to_movehut(uprn: str, address: str):
result = create_movehut_listing(uprn, address)
return {“uprn”: uprn, “movehut_response”: result}

@router.get(”/portfolio/summary”)
async def portfolio_summary(q: Optional[str] = Query(None)):
async with get_conn() as conn:
where_clause = “”
params = []
if q:
where_clause = “WHERE (p.uprn ILIKE $1 OR p.address ILIKE $1 OR p.council_name ILIKE $1 OR p.postcode ILIKE $1)”
params.append(f”%{q}%”)
sql = (
“SELECT COUNT(*) AS portfolio_count,”
“ SUM(CASE WHEN EXTRACT(DAY FROM (o.end_date - CURRENT_DATE)) BETWEEN 0 AND 10 THEN 1 ELSE 0 END) AS ends_10,”
“ SUM(CASE WHEN EXTRACT(DAY FROM (o.end_date - CURRENT_DATE)) BETWEEN 0 AND 42 THEN 1 ELSE 0 END) AS ends_6w,”
“ SUM(CASE WHEN o.end_date < CURRENT_DATE THEN 1 ELSE 0 END) AS expired”
“ FROM er_portfolio p”
“ JOIN (SELECT uprn, MAX(id) AS max_id FROM er_portfolio GROUP BY uprn) dp”
“ ON dp.uprn = p.uprn AND dp.max_id = p.id”
“ LEFT JOIN vw_mitigation_opportunities o ON o.uprn = p.uprn”
“ “ + where_clause
)
row = await conn.fetchrow(sql, *params)
return {
“portfolio_count”: int(row[“portfolio_count”] or 0),
“ends_10”: int(row[“ends_10”] or 0),
“ends_6w”: int(row[“ends_6w”] or 0),
“expired”: int(row[“expired”] or 0),
}

@router.get(”/portfolio/list”)
async def portfolio_list(q: Optional[str] = Query(None), rows: int = Query(50, ge=10, le=500)):
async with get_conn() as conn:
where_clause = “”
params = []
idx = 1
if q:
where_clause = (
f”WHERE (p.uprn ILIKE ${idx} OR p.address ILIKE ${idx}”
f” OR p.council_name ILIKE ${idx} OR p.postcode ILIKE ${idx})”
)
params.append(f”%{q}%”)
idx += 1
params.append(rows)
sql = (
“SELECT p.id, p.uprn,”
“ COALESCE(NULLIF(p.council_name,’’), ‘’) AS council,”
“ COALESCE(NULLIF(p.address,’’), ‘’) AS address,”
“ COALESCE(NULLIF(p.postcode,’’), ‘’) AS postcode,”
“ COALESCE(NULLIF(p.charge_type,’’), ‘’) AS charge_type,”
“ p.start_date, p.end_date,”
“ COALESCE(v.current_cycle, ‘ACTIVE’) AS cycle,”
“ v.cycle_start_date, v.cycle_end_date, v.days_remaining,”
“ CASE COALESCE(v.current_cycle,‘ACTIVE’)”
“   WHEN ‘MITIGATION’ THEN ‘Mitigation’”
“   WHEN ‘EMPTY’ THEN ‘Empty’”
“   ELSE ‘Active’ END AS pill_text,”
“ CASE COALESCE(v.current_cycle,‘ACTIVE’)”
“   WHEN ‘MITIGATION’ THEN ‘pill pill–warning’”
“   WHEN ‘EMPTY’ THEN ‘pill pill–danger’”
“   ELSE ‘pill pill–active’ END AS pill_class,”
“ o.end_date AS opportunity_end_date,”
“ CASE”
“   WHEN COALESCE(v.current_cycle,‘ACTIVE’) = ‘MITIGATION’ THEN v.days_remaining”
“   WHEN COALESCE(v.current_cycle,‘ACTIVE’) = ‘EMPTY’ AND v.cycle_end_date IS NOT NULL”
“     THEN EXTRACT(DAY FROM (v.cycle_end_date - CURRENT_DATE))::int”
“   WHEN o.end_date IS NOT NULL THEN EXTRACT(DAY FROM (o.end_date - CURRENT_DATE))::int”
“   ELSE NULL END AS days_to_end,”
“ CASE”
“   WHEN COALESCE(v.current_cycle,‘ACTIVE’) = ‘MITIGATION’ THEN 0”
“   WHEN COALESCE(v.current_cycle,‘ACTIVE’) = ‘EMPTY’ THEN 1”
“   WHEN o.end_date IS NOT NULL AND EXTRACT(DAY FROM (o.end_date - CURRENT_DATE)) BETWEEN 0 AND 10 THEN 2”
“   WHEN o.end_date IS NOT NULL AND EXTRACT(DAY FROM (o.end_date - CURRENT_DATE)) BETWEEN 0 AND 42 THEN 3”
“   WHEN p.start_date IS NOT NULL AND CURRENT_DATE BETWEEN p.start_date AND p.start_date + INTERVAL ‘3 months’ THEN 4”
“   WHEN o.end_date IS NOT NULL AND o.end_date < CURRENT_DATE THEN 9”
“   ELSE 5 END AS priority,”
“ CASE”
“   WHEN COALESCE(v.current_cycle,‘ACTIVE’) = ‘MITIGATION’ THEN ‘Mitigation underway’”
“   WHEN COALESCE(v.current_cycle,‘ACTIVE’) = ‘EMPTY’ THEN ‘Empty period underway’”
“   WHEN o.end_date IS NOT NULL AND o.end_date < CURRENT_DATE THEN ‘Review / escalate’”
“   WHEN o.end_date IS NOT NULL AND EXTRACT(DAY FROM (o.end_date - CURRENT_DATE)) BETWEEN 0 AND 10 THEN ‘Immediate action’”
“   WHEN o.end_date IS NOT NULL AND EXTRACT(DAY FROM (o.end_date - CURRENT_DATE)) BETWEEN 0 AND 42 THEN ‘Plan renewal’”
“   WHEN p.start_date IS NOT NULL AND CURRENT_DATE BETWEEN p.start_date AND p.start_date + INTERVAL ‘3 months’ THEN ‘Prepare next steps’”
“   ELSE ‘Monitor’ END AS next_action,”
“ CASE”
“   WHEN COALESCE(v.current_cycle,‘ACTIVE’) = ‘MITIGATION’ THEN ‘na na–window’”
“   WHEN COALESCE(v.current_cycle,‘ACTIVE’) = ‘EMPTY’ THEN ‘na na–urgent’”
“   WHEN o.end_date IS NOT NULL AND o.end_date < CURRENT_DATE THEN ‘na na–expired’”
“   WHEN o.end_date IS NOT NULL AND EXTRACT(DAY FROM (o.end_date - CURRENT_DATE)) BETWEEN 0 AND 10 THEN ‘na na–urgent’”
“   WHEN o.end_date IS NOT NULL AND EXTRACT(DAY FROM (o.end_date - CURRENT_DATE)) BETWEEN 0 AND 42 THEN ‘na na–soon’”
“   WHEN p.start_date IS NOT NULL AND CURRENT_DATE BETWEEN p.start_date AND p.start_date + INTERVAL ‘3 months’ THEN ‘na na–window’”
“   ELSE ‘na na–active’ END AS next_class,”
“ o.gross_charge, o.estimated_saving, o.value_score, o.urgency_score,”
“ CASE WHEN o.gross_charge IS NOT NULL THEN ROUND(o.gross_charge / 0.51) ELSE NULL END AS rv_approx”
“ FROM (SELECT p1.* FROM er_portfolio p1”
“   JOIN (SELECT uprn, MAX(id) AS id FROM er_portfolio GROUP BY uprn) d”
“   ON d.uprn = p1.uprn AND d.id = p1.id) p”
“ LEFT JOIN vw_portfolio_current_cycle v ON v.uarn = p.uarn”
“ LEFT JOIN (SELECT * FROM (”
“   SELECT o2.*, ROW_NUMBER() OVER (PARTITION BY o2.uprn”
“     ORDER BY COALESCE(o2.value_score,0) DESC,”
“     COALESCE(o2.estimated_saving,0) DESC, o2.end_date DESC NULLS LAST) AS rn”
“   FROM vw_mitigation_opportunities o2) r WHERE rn = 1) o ON o.uprn = p.uprn”
“ “ + where_clause +
“ ORDER BY priority ASC,”
“ COALESCE(v.cycle_end_date, o.end_date, ‘9999-12-31’::date) ASC,”
“ COALESCE(o.estimated_saving,0) DESC, p.id DESC”
f” LIMIT ${idx}”
)
records = await conn.fetch(sql, *params)
result = []
for r in records:
days = int(r[“days_to_end”]) if r[“days_to_end”] is not None else None
if days is not None:
urgency = “urgency-red” if days <= 14 else “urgency-amber” if days <= 45 else “urgency-green”
else:
urgency = “”
result.append({
“id”: r[“id”],
“uprn”: r[“uprn”],
“council”: r[“council”],
“address”: r[“address”],
“postcode”: r[“postcode”],
“charge_type”: r[“charge_type”],
“start_date”: str(r[“start_date”]) if r[“start_date”] else None,
“end_date”: str(r[“end_date”]) if r[“end_date”] else None,
“opportunity_end_date”: str(r[“opportunity_end_date”]) if r[“opportunity_end_date”] else None,
“cycle”: r[“cycle”],
“cycle_start_date”: str(r[“cycle_start_date”]) if r[“cycle_start_date”] else None,
“cycle_end_date”: str(r[“cycle_end_date”]) if r[“cycle_end_date”] else None,
“days_remaining”: r[“days_remaining”],
“pill_text”: r[“pill_text”],
“pill_class”: r[“pill_class”],
“days_to_end”: days,
“urgency_class”: urgency,
“priority”: r[“priority”],
“next_action”: r[“next_action”],
“next_class”: r[“next_class”],
“gross_charge”: float(r[“gross_charge”]) if r[“gross_charge”] else None,
“estimated_saving”: float(r[“estimated_saving”]) if r[“estimated_saving”] else None,
“rv_approx”: float(r[“rv_approx”]) if r[“rv_approx”] else None,
})
return {“count”: len(result), “rows”: result}
