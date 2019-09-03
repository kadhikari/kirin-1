"""
Table contributor referenced in real_time_update and trip_update
Revision ID: 443587af1780
Revises: 53647ffeb3c6
Create Date: 2019-08-29 10:28:48.053647

"""
from __future__ import absolute_import, print_function, unicode_literals, division

# revision identifiers, used by Alembic.
revision = "443587af1780"
down_revision = "53647ffeb3c6"

from alembic import op
import sqlalchemy as sa


def upgrade():
    # Insert information for a contributor 'realtime.cots' and 'realtime.sherbrooke'
    # if present in the table trip_update and absent in contributor
    op.execute(
        "INSERT INTO contributor SELECT 'realtime.cots','sncf',"
        "'token_to_be_modified','feed_url_to_be_modified','cots'"
        " WHERE 'realtime.cots' in (SELECT DISTINCT contributor FROM real_time_update)"
        " AND NOT EXISTS (SELECT * FROM contributor WHERE ID = 'realtime.cots');"
    )
    op.execute(
        "INSERT INTO contributor SELECT 'realtime.sherbrooke','ca-qc-sherbrooke',"
        "'token_to_be_modified','feed_url_to_be_modified','gtfs-rt'"
        " WHERE 'realtime.sherbrooke' in (SELECT DISTINCT contributor FROM real_time_update)"
        " AND NOT EXISTS (SELECT * FROM contributor WHERE ID = 'realtime.sherbrooke');"
    )

    # add contributor_id in real_time_update and trip_update as foreign key
    op.add_column("real_time_update", sa.Column("contributor_id", sa.Text(), nullable=True))
    op.create_foreign_key(
        "fk_real_time_update_contributor_id", "real_time_update", "contributor", ["contributor_id"], ["id"]
    )
    op.add_column("trip_update", sa.Column("contributor_id", sa.Text(), nullable=True))
    op.create_foreign_key(
        "fk_trip_update_contributor_id", "trip_update", "contributor", ["contributor_id"], ["id"]
    )

    # Update contributor_id value with contributor
    op.execute("UPDATE real_time_update SET contributor_id = contributor;")
    op.execute("UPDATE trip_update SET contributor_id = contributor;")

    # Modify nullable property to False
    op.execute("ALTER TABLE real_time_update ALTER COLUMN contributor_id SET NOT NULL;")
    op.execute("ALTER TABLE trip_update ALTER COLUMN contributor_id SET NOT NULL;")

    # create index on contributor_id
    op.create_index(
        "realtime_update_contributor_id_and_created_at",
        "real_time_update",
        ["created_at", "contributor_id"],
        unique=False,
    )
    op.create_index("contributor_id_idx", "trip_update", ["contributor_id"], unique=False)


def downgrade():
    # Drop index on contributor_id
    op.drop_index("realtime_update_contributor_id_and_created_at", table_name="real_time_update")
    op.drop_index("contributor_id_idx", table_name="trip_update")

    # Drop foreignKey constraints
    op.drop_constraint("fk_trip_update_contributor_id", "trip_update", type_="foreignkey")
    op.drop_constraint("fk_real_time_update_contributor_id", "real_time_update", type_="foreignkey")

    # Delete contributor_id in real_time_update and trip_update
    op.drop_column("trip_update", "contributor_id")
    op.drop_column("real_time_update", "contributor_id")

    # Delete lines from the table contributor
    op.execute("DELETE FROM contributor;")
