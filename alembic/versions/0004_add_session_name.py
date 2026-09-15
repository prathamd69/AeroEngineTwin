"""add session_name to simulation_runs

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-16

"""
from alembic import op
import sqlalchemy as sa

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "simulation_runs",
        sa.Column("session_name", sa.String(255), nullable=False, server_default=""),
    )
    op.alter_column("simulation_runs", "session_name", server_default=None)


def downgrade() -> None:
    op.drop_column("simulation_runs", "session_name")