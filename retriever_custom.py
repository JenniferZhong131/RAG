# retriever.py  — template routing + evaluation
import json, os, sqlite3
from typing import Dict, List, Tuple
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

DB = "data/app.db"
TRUTH = "truth/qna.json"

# --- 10 个模板（与 truth/make_truth.py 中的 SQL 完全一致） ---
TEMPLATES: Dict[str, str] = {
    "Top-3 boroughs by request volume in the last 30 days": """
WITH mx AS (SELECT MAX(created_date) m FROM nyc_311)
SELECT borough, COUNT(*) AS cnt
FROM nyc_311, mx
WHERE borough IS NOT NULL
  AND created_date >= datetime(mx.m, '-30 days')
GROUP BY borough
ORDER BY cnt DESC
LIMIT 3;""",

    "Top-10 complaint_type in the last 12 months with shares": """
WITH mx AS (SELECT MAX(created_date) m FROM nyc_311),
     f AS (
       SELECT complaint_type
       FROM nyc_311, mx
       WHERE created_date >= datetime(mx.m, '-12 months')
     )
SELECT complaint_type, COUNT(*) AS cnt,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM f), 2) AS pct
FROM f
GROUP BY complaint_type
ORDER BY cnt DESC
LIMIT 10;""",

    "Borough with the shortest average close time (hours)": """
SELECT borough,
       ROUND(AVG((julianday(closed_date) - julianday(created_date)) * 24), 2) AS avg_hours
FROM nyc_311
WHERE borough IS NOT NULL AND closed_date IS NOT NULL
GROUP BY borough
ORDER BY avg_hours ASC
LIMIT 1;""",

    "Monthly trend of 'Noise'-related requests by borough (last 12 months)": """
WITH mx AS (SELECT MAX(created_date) m FROM nyc_311),
     f AS (
       SELECT created_date, borough
       FROM nyc_311, mx
       WHERE created_date >= datetime(mx.m, '-12 months')
         AND borough IS NOT NULL
         AND (complaint_type LIKE '%Noise%' OR descriptor LIKE '%Noise%')
     )
SELECT strftime('%Y-%m', created_date) AS month, borough, COUNT(*) AS cnt
FROM f
GROUP BY month, borough
ORDER BY month, borough;""",

    "Share of requests with status = 'Closed' for ZIPs starting with 100": """
SELECT ROUND(100.0 * SUM(CASE WHEN status='Closed' THEN 1 ELSE 0 END) / COUNT(*), 2) AS closed_pct
FROM nyc_311
WHERE incident_zip LIKE '100%';""",

    "Top-5 countries by average points": """
SELECT country, ROUND(AVG(points), 2) AS avg_pts, COUNT(*) AS n
FROM wine_reviews
WHERE country IS NOT NULL
GROUP BY country
ORDER BY avg_pts DESC
LIMIT 5;""",

    "Average points by price buckets": """
SELECT bucket, ROUND(AVG(points), 2) AS avg_pts, COUNT(*) AS n
FROM (
  SELECT CASE
           WHEN price>=10 AND price<20  THEN '[10,20)'
           WHEN price>=20 AND price<50  THEN '[20,50)'
           WHEN price>=50 AND price<100 THEN '[50,100)'
           WHEN price>=100             THEN '[100,+)'
           ELSE 'other'
         END AS bucket, points
  FROM wine_reviews
  WHERE price IS NOT NULL
)
GROUP BY bucket
ORDER BY CASE bucket
  WHEN '[10,20)' THEN 1
  WHEN '[20,50)' THEN 2
  WHEN '[50,100)' THEN 3
  WHEN '[100,+)' THEN 4
  ELSE 5 END;""",

    "Top-5 varieties by average points with n ≥ 500": """
SELECT variety, COUNT(*) AS n, ROUND(AVG(points), 2) AS avg_pts
FROM wine_reviews
WHERE variety IS NOT NULL
GROUP BY variety
HAVING n >= 500
ORDER BY avg_pts DESC
LIMIT 5;""",

    "Overall missing price percentage": """
SELECT ROUND(100.0 * SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS missing_price_pct
FROM wine_reviews;""",

    "Top-10 countries by count of missing price": """
SELECT country, COUNT(*) AS missing
FROM wine_reviews
WHERE price IS NULL
GROUP BY country
ORDER BY missing DESC
LIMIT 10;""",
}

# 用模板名称作为“标签句子”，用 TF-IDF 选择最像的模板
LABELS = list(TEMPLATES.keys())
VEC = TfidfVectorizer(max_features=50000, ngram_range=(1,2))
X = VEC.fit_transform(LABELS)

def pick_template(question: str) -> str:
    qx = VEC.transform([question])
    sim = cosine_similarity(qx, X)[0]
    return LABELS[int(sim.argmax())]

def rows_equal(got: List[Tuple], exp: List[Tuple]) -> bool:
    return got == exp  # 真值对比要求完全一致

def main():
    with open(TRUTH, "r", encoding="utf-8") as f:
        truth = json.load(f)

    con = sqlite3.connect(DB); cur = con.cursor()
    passed = 0

    for i, item in enumerate(truth["items"], 1):
        q = item["question"]
        exp = [tuple(r) for r in item["expected_rows"]]

        label = pick_template(q)
        sql = TEMPLATES[label]

        cur.execute(sql)
        got = cur.fetchall()

        ok = rows_equal(got, exp)
        passed += int(ok)

        print(f"[{'PASS' if ok else 'FAIL'}] {q}")
        if not ok:
            print("  picked:", label)
            print("  sql   :", sql.strip())
            print("  expected:", exp[:3])
            print("  got     :", got[:3])

    acc = passed / len(truth["items"])
    print(f"\nAccuracy: {passed}/{len(truth['items'])} = {acc:.2%}")

if __name__ == "__main__":
    main()
