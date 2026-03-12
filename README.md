# 🏠 Pawperty — Property Ledger Blockchain

A blockchain-backed property registration and management system built with **FastAPI**, **SQLAlchemy**, and a custom Python blockchain. Features role-based access for admins, officers, and citizens with a neobrutalist UI.

---

## Features

### 🔗 Blockchain Ledger

- Custom blockchain with encrypted SQLite persistence(decentralised deployment will use dockerized postgres)
- Property **registration**, **transfer** (sale), and **inheritance**
- Immutable ownership history with full audit trail
- Automatic stamp duty and registration fee calculation
- Chain validation and block explorer

### 👤 Role-Based Access

| Role        | Access                                                                     |
| ----------- | -------------------------------------------------------------------------- |
| **Admin**   | User management, chain validation, block explorer, all property operations |
| **Officer** | Property registration, transfer, inheritance, search, analytics dashboard  |
| **Citizen** | View own current & past properties, read-only property detail with history |

### 📊 Officer Analytics

- Transaction frequency polygon (registrations, transfers, inheritances)
- Property value heatmap by district with intensity bars

### 🔍 Smart Features

- CUST-ID lookup during transfers/inheritances — auto-fills owner data from blockchain or citizen table
- Fuzzy property search across multiple fields
- HTMX-powered inline validation (Aadhar, PAN, survey number uniqueness)
- Live stamp duty preview on transfer forms

---

## Tech Stack

| Layer    | Technology                                              |
| -------- | ------------------------------------------------------- |
| Backend  | Python 3.10+, FastAPI, Uvicorn                          |
| Database | PostgreSQL (users/citizens), SQLite (blockchain ledger) |
| ORM      | SQLAlchemy                                              |
| Auth     | bcrypt password hashing, session cookies                |
| Frontend | Jinja2 templates, HTMX, Alpine.js                       |
| Charts   | Chart.js                                                |
| Design   | Neobrutalist CSS (custom design system)                 |

---

## Project Structure

```
Pawperty/
├── app.py                  # FastAPI application — routes, middleware
├── auth.py                 # Password hashing, session management, role guards
├── Blockchain.py           # Custom blockchain — Block, Owner, PropertyBlockchain
├── database.py             # PostgreSQL connection & session factory
├── models.py               # SQLAlchemy models (User, Citizen) + Pydantic schemas
├── requirements.txt        # Python dependencies
├── pawperty_ledger.db      # Encrypted SQLite blockchain database
├── static/
│   └── css/
│       ├── style.css       # Main design system (neobrutalist)
│       └── forms.css       # Form component styles
├── templates/
│   ├── base.html           # Base layout with nav, flash messages, footer
│   ├── login.html          # Staff + Citizen login (tab switcher)
│   ├── index.html          # Officer/Admin dashboard
│   ├── analytics.html      # Officer analytics (chart + heatmap)
│   ├── properties.html     # Property list
│   ├── property_detail.html# Property detail + history
│   ├── register.html       # Property registration form
│   ├── transfer.html       # Property transfer form (with CUST-ID lookup)
│   ├── inherit.html        # Property inheritance form (with CUST-ID lookup)
│   ├── search.html         # Property search
│   ├── citizen/
│   │   ├── dashboard.html  # Citizen's property portfolio
│   │   ├── register.html   # Citizen self-registration
│   │   └── property_detail.html  # Read-only property view
│   └── partials/
│       ├── nav.html        # Navigation bar (role-aware)
│       ├── icons.html      # SVG icon macros
│       ├── flash.html      # Flash messages
│       └── ...             # Other HTMX partials
```

---

## Setup

### Prerequisites

- Python 3.10+
- PostgreSQL running locally
- pip or venv

### Installation

```bash
# Clone the repo
git clone <repo-url>
cd Pawperty

# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

# Install dependencies
pip install -r requirements.txt
```

### Database Configuration

Edit the PostgreSQL connection string in `database.py`:

```python
DATABASE_URL = "postgresql://username:password@localhost:5432/pawperty"
```

The database tables are auto-created on first run via `Base.metadata.create_all()`.

### Run

```bash
python -m uvicorn app:app --reload
```

The app starts at **http://localhost:8000**.

### Default Admin

On first startup, a default admin account is created:

- **Username:** `admin`
- **Password:** `admin123`

> ⚠️ Change the default password immediately after first login.

---

## User Flows

### Officer: Register a Property

1. Login → Navigate to **Register**
2. Fill owner details (or use CUST-ID lookup) + property details
3. Submit → Property is recorded on the blockchain

### Officer: Transfer Property

1. Navigate to **Transfer**
2. Enter property key + new owner details (or use CUST-ID lookup)
3. Set transfer value → stamp duty auto-calculates
4. Submit → Ownership changes on the blockchain

### Citizen: Register & View Properties

1. Go to `/login` → Switch to **Citizen Login** tab → Click **Register as Citizen**
2. Enter name, Aadhar, PAN, password (optionally link existing CUST-ID)
3. After registration, the dashboard shows:
   - **My Properties** — currently owned
   - **Property History** — previously owned (transferred away)

---

## License

This project is for educational purposes.
