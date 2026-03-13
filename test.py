from fastapi.testclient import TestClient
import app as a
a.app.dependency_overrides[a.require_officer] = lambda: a.User(id=1, role='officer', username='test')
with TestClient(a.app) as c:
    r = c.get('/corrections/new?property_key=PRO-000002')
    print('GET /corrections/new ->', r.status_code)
