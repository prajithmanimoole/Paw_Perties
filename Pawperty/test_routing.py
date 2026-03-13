from fastapi import FastAPI
from fastapi.testclient import TestClient

app = FastAPI()

@app.get("/test", status_code=200)
def t1(): return "list"

@app.get("/test/new", status_code=200)
def t2(): return "new"

@app.get("/test/{id}", status_code=200)
def t3(): return "id"

c = TestClient(app)
print('GET /test ->', c.get('/test').json())
print('GET /test/new ->', c.get('/test/new').json())
print('GET /test/123 ->', c.get('/test/123').json())
