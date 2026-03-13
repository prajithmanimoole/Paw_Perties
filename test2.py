from fastapi.testclient import TestClient
import app as a

a.app.dependency_overrides[a.require_officer] = lambda: a.User(id=1, role='officer', username='test_usr')

with TestClient(a.app) as c:
    r = c.get('/corrections/new?property_key=PRO-000002')
    print('Testing exact URL /corrections/new')
    print('Status:', r.status_code)
    # Check if the title has "New Correction Request" to prove it hit the right page
    if b'New Correction Request' in r.content:
        print('Matched correction_new_page')
    elif b'Correction Request' in r.content:
        print('Matched correction_detail (but with right content??)')
    else:
        print('Content does not match new page')
