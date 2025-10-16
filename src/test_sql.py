from langchain_community.utilities import SQLDatabase

db = SQLDatabase.from_uri("sqlite:///data/app.db")

print("NYC rows:", db.run("SELECT COUNT(*) FROM nyc_311"))
print("WINE rows:", db.run("SELECT COUNT(*) FROM wine_reviews"))

print(db.run("""
SELECT borough, COUNT(*) AS cnt
FROM nyc_311
GROUP BY borough
ORDER BY cnt DESC
LIMIT 5
"""))

print(db.run("""
SELECT country, ROUND(AVG(points),2) AS avg_pts, COUNT(*) AS n
FROM wine_reviews
GROUP BY country
HAVING n >= 100
ORDER BY avg_pts DESC
LIMIT 5
"""))
