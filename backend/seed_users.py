"""Seed demo users: admin_user, analyst_user, viewer_user"""
from app.db.database import SessionLocal
from app.models.models import User, Base
from app.db.database import engine
from app.services.auth import hash_password

# Ensure tables exist
Base.metadata.create_all(bind=engine)

db = SessionLocal()

demo_users = [
    {"username": "admin_user",   "email": "admin@datahub.io",   "password": "Admin@123",   "role": "admin"},
    {"username": "analyst_user", "email": "analyst@datahub.io", "password": "Analyst@123", "role": "analyst"},
    {"username": "viewer_user",  "email": "viewer@datahub.io",  "password": "Viewer@123",  "role": "viewer"},
]

for u in demo_users:
    existing = db.query(User).filter(User.username == u["username"]).first()
    if existing:
        print(f"User {u['username']} already exists — updating password & role.")
        existing.hashed_password = hash_password(u["password"])
        existing.role = u["role"]
    else:
        user = User(
            username=u["username"],
            email=u["email"],
            hashed_password=hash_password(u["password"]),
            role=u["role"],
        )
        db.add(user)
        print(f"Created: {u['username']} ({u['role']})")

db.commit()
db.close()
print("Done! Demo users seeded successfully.")
