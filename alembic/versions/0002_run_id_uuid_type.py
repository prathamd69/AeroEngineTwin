"""convert run_id to native uuid type

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-16

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "simulations",
        "run_id",
        type_=postgresql.UUID(as_uuid=True),
        postgresql_using="run_id::uuid",
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "simulations",
        "run_id",
        type_=sa.String(50),
        postgresql_using="run_id::text",
        nullable=False,
    )