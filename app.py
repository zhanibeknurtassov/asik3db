from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Text, Numeric,
    func, Date, Time, ForeignKey, select, and_, update, case, delete, cast
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func as sql_func
from datetime import date, time
import json
from sqlalchemy import Integer as SQLInteger

DB_URL = "postgresql://postgres:1234@localhost:5433/postgres"
engine = create_engine(DB_URL)


def create_tables():
    # 1. Create SQL Statements
    metadata = MetaData()

    USER = Table(
        "USER", metadata,
        Column("user_id", Integer, primary_key=True, autoincrement=True),
        Column("email", String(255), unique=True, nullable=False),
        Column("given_name", String(100), nullable=False),
        Column("surname", String(100), nullable=False),
        Column("city", String(100)),
        Column("phone_number", String(20)),
        Column("profile_description", Text),
        Column("password", String(255), nullable=False)
    )

    CAREGIVER = Table(
        "CAREGIVER", metadata,
        Column("caregiver_user_id", Integer, ForeignKey("USER.user_id", ondelete="CASCADE"), primary_key=True),
        Column("photo", Text),
        Column("gender", String(20)),
        Column("caregiving_type", String(100)),
        Column("hourly_rate", Numeric(10, 2))
    )

    MEMBER = Table(
        "MEMBER", metadata,
        Column("member_user_id", Integer, ForeignKey("USER.user_id", ondelete="CASCADE"), primary_key=True),
        Column("house_rules", Text),
        Column("dependent_description", Text)
    )

    ADDRESS = Table(
        "ADDRESS", metadata,
        Column("member_user_id", Integer, ForeignKey("MEMBER.member_user_id", ondelete="CASCADE"), primary_key=True),
        Column("house_number", String(50)),
        Column("street", String(255)),
        Column("town", String(100))
    )

    JOB = Table(
        "JOB", metadata,
        Column("job_id", Integer, primary_key=True, autoincrement=True),
        Column("member_user_id", Integer, ForeignKey("MEMBER.member_user_id", ondelete="CASCADE"), nullable=False),
        Column("required_caregiving_type", String(100)),
        Column("other_requirements", Text),
        Column("date_posted", Date, server_default=func.current_date())
    )

    JOB_APPLICATION = Table(
        "JOB_APPLICATION", metadata,
        Column("caregiver_user_id", Integer, ForeignKey("CAREGIVER.caregiver_user_id", ondelete="CASCADE"), primary_key=True),
        Column("job_id", Integer, ForeignKey("JOB.job_id", ondelete="CASCADE"), primary_key=True),
        Column("date_applied", Date, server_default=func.current_date())
    )

    APPOINTMENT = Table(
        "APPOINTMENT", metadata,
        Column("appointment_id", Integer, primary_key=True, autoincrement=True),
        Column("caregiver_user_id", Integer, ForeignKey("CAREGIVER.caregiver_user_id", ondelete="CASCADE"), nullable=False),
        Column("member_user_id", Integer, ForeignKey("MEMBER.member_user_id", ondelete="CASCADE"), nullable=False),
        Column("appointment_date", Date, nullable=False),
        Column("appointment_time", Time, nullable=False),
        Column("work_hours", Numeric(5,2)),
        Column("status", String(50))
    )

    metadata.create_all(engine)
    print("Tables created/ensured.")
    return engine, metadata, {
    "USER": USER,
    "CAREGIVER": CAREGIVER,
    "MEMBER": MEMBER,
    "ADDRESS": ADDRESS,
    "JOB": JOB,
    "JOB_APPLICATION": JOB_APPLICATION,
    "APPOINTMENT": APPOINTMENT
}


def _reflect_tables():
    # helper function
    metadata = MetaData()
    metadata.reflect(bind=engine)
    t = metadata.tables
    # convenience: return commonly used tables
    return {
        "metadata": metadata,
        "USER": t["USER"],
        "CAREGIVER": t["CAREGIVER"],
        "MEMBER": t["MEMBER"],
        "ADDRESS": t["ADDRESS"],
        "JOB": t["JOB"],
        "JOB_APPLICATION": t["JOB_APPLICATION"],
        "APPOINTMENT": t["APPOINTMENT"],
    }


def seed_data(data_path="data.json"):
    # 2. Insert SQL Statements using a seeder from json
    tables = _reflect_tables()
    USER = tables["USER"]
    CAREGIVER = tables["CAREGIVER"]
    MEMBER = tables["MEMBER"]
    ADDRESS = tables["ADDRESS"]
    JOB = tables["JOB"]
    JOB_APPLICATION = tables["JOB_APPLICATION"]
    APPOINTMENT = tables["APPOINTMENT"]

    # load data
    with open(data_path, encoding="utf-8-sig") as f:
        data = json.load(f)

    # convert date/time strings to python objects
    for j in data.get("jobs", []):
        if j.get("date_posted"):
            j["date_posted"] = date.fromisoformat(j["date_posted"])
    for ja in data.get("job_applications", []):
        if ja.get("date_applied"):
            ja["date_applied"] = date.fromisoformat(ja["date_applied"])
    for ap in data.get("appointments", []):
        if ap.get("appointment_date"):
            ap["appointment_date"] = date.fromisoformat(ap["appointment_date"])
        if ap.get("appointment_time"):
            h, m = map(int, ap["appointment_time"].split(":"))
            ap["appointment_time"] = time(hour=h, minute=m)

    with engine.begin() as conn:
        # bulk insert lists of dicts with ON CONFLICT DO NOTHING, it makes it idempotent
        if data.get("users"):
            stmt = insert(USER).values(data["users"])
            stmt = stmt.on_conflict_do_nothing(index_elements=["user_id"])
            conn.execute(stmt)

        if data.get("caregivers"):
            stmt = insert(CAREGIVER).values(data["caregivers"])
            stmt = stmt.on_conflict_do_nothing(index_elements=["caregiver_user_id"])
            conn.execute(stmt)

        if data.get("members"):
            stmt = insert(MEMBER).values(data["members"])
            stmt = stmt.on_conflict_do_nothing(index_elements=["member_user_id"])
            conn.execute(stmt)

        if data.get("addresses"):
            stmt = insert(ADDRESS).values(data["addresses"])
            stmt = stmt.on_conflict_do_nothing(index_elements=["member_user_id"])
            conn.execute(stmt)

        if data.get("jobs"):
            stmt = insert(JOB).values(data["jobs"])
            stmt = stmt.on_conflict_do_nothing(index_elements=["job_id"])
            conn.execute(stmt)

        if data.get("job_applications"):
            stmt = insert(JOB_APPLICATION).values(data["job_applications"])
            stmt = stmt.on_conflict_do_nothing(index_elements=["caregiver_user_id", "job_id"])
            conn.execute(stmt)

        if data.get("appointments"):
            stmt = insert(APPOINTMENT).values(data["appointments"])
            stmt = stmt.on_conflict_do_nothing(index_elements=["appointment_id"])
            conn.execute(stmt)

    print("Seed complete.")

def fix_all_sequences():
    tables_with_pk = [
        ("USER", "user_id"),
        ("CAREGIVER", "caregiver_user_id"),
        ("MEMBER", "member_user_id"),
        ("ADDRESS", "member_user_id"),
        ("JOB", "job_id"),
        ("JOB_APPLICATION", "caregiver_user_id"),  
        ("APPOINTMENT", "appointment_id"),
    ]
    with engine.begin() as conn:
        for table_name, pk_name in tables_with_pk:
            tbl = metadata.tables[table_name]
            max_id = conn.execute(select(func.max(tbl.c[pk_name]))).scalar() or 0
            seq_name = f"{table_name}_{pk_name}_seq"
            try:
                conn.execute(f'ALTER SEQUENCE "{seq_name}" RESTART WITH {max_id + 1}')
            except Exception:
                pass




def update_arman_phone():
    # 3.1 Update SQL Statement
    tables = _reflect_tables()
    USER = tables["USER"]
    with engine.begin() as conn:
        stmt = (
            update(USER)
            .where(USER.c.given_name == "Arman")
            .where(USER.c.surname == "Armanov")
            .values(phone_number="+77773414141")
        )
        res = conn.execute(stmt)
        print(f"Arman phone update affected {res.rowcount} row(s).")


def update_caregiver_rates(round_to_2decimals=False):
    # 3.2 Update SQL Statement
    tables = _reflect_tables()
    CAREGIVER = tables["CAREGIVER"]

    with engine.begin() as conn:
        new_rate_expr = case(
            (CAREGIVER.c.hourly_rate < 10, CAREGIVER.c.hourly_rate + 0.3),
            else_=CAREGIVER.c.hourly_rate * 1.10
        )
        if round_to_2decimals:
            # cast to numeric is DB-side rounding/truncation; using ::numeric(10,2)
            # SQLAlchemy cast to Numeric could be used, but simplest is cast(..., SQLInteger) was for integer earlier.
            from sqlalchemy import Numeric as SqlNumeric
            new_rate_expr = cast(new_rate_expr, SqlNumeric(10,2))

        stmt = update(CAREGIVER).values(hourly_rate=new_rate_expr)
        res = conn.execute(stmt)
        print(f"Hourly rates update affected {res.rowcount} row(s).")


def delete_jobs_by_amina():
    # 4.1 Delete the jobs posted by Amina Aminova.
    tables = _reflect_tables()
    USER = tables["USER"]
    JOB = tables["JOB"]

    with engine.begin() as conn:
        amina_user_id = conn.execute(
            select(USER.c.user_id).where(
                (USER.c.given_name == "Amina") & (USER.c.surname == "Aminova")
            )
        ).scalar()

        if amina_user_id:
            res = conn.execute(delete(JOB).where(JOB.c.member_user_id == amina_user_id))
            print(f"Deleted {res.rowcount} job(s) posted by Amina Aminova.")
        else:
            print("Amina Aminova not found; no jobs deleted.")


def delete_members_on_kabanbay():
    # 4.2 Delete all members who live on Kabanbay Batyr street. 
    tables = _reflect_tables()
    MEMBER = tables["MEMBER"]
    ADDRESS = tables["ADDRESS"]

    with engine.begin() as conn:
        member_ids = conn.execute(
            select(ADDRESS.c.member_user_id).where(ADDRESS.c.street == "Kabanbay Batyr")
        ).scalars().all()

        if member_ids:
            res = conn.execute(delete(MEMBER).where(MEMBER.c.member_user_id.in_(member_ids)))
            print(f"Deleted {res.rowcount} member(s) who lived on Kabanbay Batyr.")
        else:
            print("No members found on Kabanbay Batyr.")


def selects_5_x():
    # 5. Simple Queries
    tables = _reflect_tables()
    USER = tables["USER"]
    MEMBER = tables["MEMBER"]
    CAREGIVER = tables["CAREGIVER"]
    APPOINTMENT = tables["APPOINTMENT"]
    JOB = tables["JOB"]
    ADDRESS = tables["ADDRESS"]

    out = {}
    with engine.connect() as conn:
        # 5.1 caregiver & member names for accepted appointments
        caregiver_user = USER.alias("caregiver")
        member_user = USER.alias("member")

        stmt_5_1 = (
            select(
                caregiver_user.c.given_name.label("caregiver_name"),
                member_user.c.given_name.label("member_name")
            )
            .select_from(
                APPOINTMENT
                .join(CAREGIVER, APPOINTMENT.c.caregiver_user_id == CAREGIVER.c.caregiver_user_id)
                .join(caregiver_user, CAREGIVER.c.caregiver_user_id == caregiver_user.c.user_id)
                .join(MEMBER, APPOINTMENT.c.member_user_id == MEMBER.c.member_user_id)
                .join(member_user, MEMBER.c.member_user_id == member_user.c.user_id)
            )
            .where(APPOINTMENT.c.status == "accepted")
        )
        out["5.1"] = conn.execute(stmt_5_1).all()

        # 5.2 job ids containing 'soft-spoken'
        stmt_5_2 = select(JOB.c.job_id).where(JOB.c.other_requirements.ilike("%soft-spoken%"))
        out["5.2"] = conn.execute(stmt_5_2).all()

        # 5.3 work hours of babysitter positions (child)
        stmt_5_3 = select(APPOINTMENT.c.work_hours).join(
            JOB, APPOINTMENT.c.member_user_id == JOB.c.member_user_id
        ).where(JOB.c.required_caregiving_type.ilike("%child%"))
        out["5.3"] = conn.execute(stmt_5_3).all()

        # 5.4 Members looking for Elderly Care in Astana with 'No pets' rule
        stmt_5_4 = select(USER.c.given_name, USER.c.surname).select_from(
            MEMBER.join(JOB, MEMBER.c.member_user_id == JOB.c.member_user_id)
                  .join(USER, MEMBER.c.member_user_id == USER.c.user_id)
                  .join(ADDRESS, MEMBER.c.member_user_id == ADDRESS.c.member_user_id)
        ).where(
            and_(
                JOB.c.required_caregiving_type.ilike("%elderly%"),
                ADDRESS.c.town.ilike("%Astana%"),
                MEMBER.c.house_rules.ilike("%No pets%")
            )
        )
        out["5.4"] = conn.execute(stmt_5_4).all()

    return out


def queries_6_x():
    # 6. Complex Queries
    tables = _reflect_tables()
    USER = tables["USER"]
    CAREGIVER = tables["CAREGIVER"]
    APPOINTMENT = tables["APPOINTMENT"]
    JOB = tables["JOB"]
    JOB_APPLICATION = tables["JOB_APPLICATION"]

    out = {}
    with engine.begin() as conn:
        # 6.1 Count applicants per job
        stmt_6_1 = (
            select(
                JOB.c.job_id,
                sql_func.count(JOB_APPLICATION.c.caregiver_user_id).label("num_applicants")
            )
            .outerjoin(JOB_APPLICATION, JOB.c.job_id == JOB_APPLICATION.c.job_id)
            .group_by(JOB.c.job_id)
        )
        out["6.1"] = conn.execute(stmt_6_1).all()

        # 6.2 Total hours spent by caregivers for accepted appointments (per caregiver)
        stmt_6_2 = (
            select(
                CAREGIVER.c.caregiver_user_id,
                sql_func.sum(APPOINTMENT.c.work_hours).label("total_hours")
            )
            .join(APPOINTMENT, CAREGIVER.c.caregiver_user_id == APPOINTMENT.c.caregiver_user_id)
            .where(APPOINTMENT.c.status.ilike("accepted"))
            .group_by(CAREGIVER.c.caregiver_user_id)
        )
        out["6.2"] = conn.execute(stmt_6_2).all()

        # 6.3 Average pay of caregivers based on accepted appointments
        stmt_6_3 = (
            select(
                sql_func.avg(CAREGIVER.c.hourly_rate * APPOINTMENT.c.work_hours).label("avg_pay")
            )
            .join(APPOINTMENT, CAREGIVER.c.caregiver_user_id == APPOINTMENT.c.caregiver_user_id)
            .where(APPOINTMENT.c.status.ilike("accepted"))
        )
        out["6.3"] = conn.execute(stmt_6_3).scalar()

        # 6.4 Caregivers who earn above average based on accepted appointments
        sub_avg_pay = (
            select(sql_func.avg(CAREGIVER.c.hourly_rate * APPOINTMENT.c.work_hours))
            .join(APPOINTMENT, CAREGIVER.c.caregiver_user_id == APPOINTMENT.c.caregiver_user_id)
            .where(APPOINTMENT.c.status.ilike("accepted"))
            .scalar_subquery()
        )

        stmt_6_4 = (
            select(
                USER.c.given_name,
                USER.c.surname,
                sql_func.sum(CAREGIVER.c.hourly_rate * APPOINTMENT.c.work_hours).label("total_pay")
            )
            .join(CAREGIVER, USER.c.user_id == CAREGIVER.c.caregiver_user_id)
            .join(APPOINTMENT, CAREGIVER.c.caregiver_user_id == APPOINTMENT.c.caregiver_user_id)
            .where(APPOINTMENT.c.status.ilike("accepted"))
            .group_by(USER.c.user_id)
            .having(sql_func.sum(CAREGIVER.c.hourly_rate * APPOINTMENT.c.work_hours) > sub_avg_pay)
        )
        out["6.4"] = conn.execute(stmt_6_4).all()

    return out


def total_cost_per_caregiver(cast_to_int=False):
    # 7. Query with a Derived Attribute

    tables = _reflect_tables()
    USER = tables["USER"]
    CAREGIVER = tables["CAREGIVER"]
    APPOINTMENT = tables["APPOINTMENT"]

    with engine.connect() as conn:
        expr = sql_func.sum(CAREGIVER.c.hourly_rate * APPOINTMENT.c.work_hours)
        if cast_to_int:
            expr = cast(expr, SQLInteger)

        stmt = (
            select(
                USER.c.given_name,
                USER.c.surname,
                expr.label("total_cost")
            )
            .select_from(USER)
            .join(CAREGIVER, USER.c.user_id == CAREGIVER.c.caregiver_user_id)
            .join(APPOINTMENT, CAREGIVER.c.caregiver_user_id == APPOINTMENT.c.caregiver_user_id)
            .where(APPOINTMENT.c.status.ilike("accepted"))
            .group_by(USER.c.user_id)
        )
        return conn.execute(stmt).all()


def view_job_applications():
    # 8. View Operation
    tables = _reflect_tables()
    JOB = tables["JOB"]
    JOB_APPLICATION = tables["JOB_APPLICATION"]
    CAREGIVER = tables["CAREGIVER"]
    USER = tables["USER"]

    with engine.connect() as conn:
        stmt = (
            select(
                JOB.c.job_id,
                USER.c.given_name,
                USER.c.surname
            )
            .select_from(JOB)
            .join(JOB_APPLICATION, JOB.c.job_id == JOB_APPLICATION.c.job_id)
            .join(CAREGIVER, JOB_APPLICATION.c.caregiver_user_id == CAREGIVER.c.caregiver_user_id)
            .join(USER, CAREGIVER.c.caregiver_user_id == USER.c.user_id)
        )
        return conn.execute(stmt).all()

engine, metadata, tables = create_tables()
seed_data()

print("eoisefojefjseopfjoisjefoijesfio\n\n\n\n\n\n")
print(tables["USER"].columns.keys())
# FLASK
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import datetime
from sqlalchemy.exc import SQLAlchemyError

fix_all_sequences()
app = Flask(__name__)

CORS(app)

import traceback

@app.route("/<table_name>", methods=["POST"])
def create_record(table_name):
    print("→ POST hit for table:", table_name)
    print("Current tables keys:", list(tables.keys()))
    tbl = globals().get("tables", {}).get(table_name.upper())
    print("Resolved table object:", tbl)
    if tbl is None:
        return jsonify({"error": "Table not found"}), 404
    data = request.json or {}
    print("Received JSON data:", data)
    try:
        with engine.begin() as conn:   
            stmt = insert(tbl).values(data)         
            pk_names = [c.name for c in tbl.primary_key.columns]
            print("Primary key columns:", pk_names)
            
            if all(k in data for k in pk_names):
                stmt = stmt.on_conflict_do_nothing(index_elements=pk_names)
            conn.execute(stmt)
        return jsonify({"status": "success"})
    except Exception as e:
        print("FULL ERROR TRACEBACK:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500 


# READ
@app.route("/<table_name>", methods=["GET"])
def read_records(table_name):
    table = tables.get(table_name)
    if table is None:
        return jsonify({"error": "Table not found"}), 404

    with engine.connect() as conn:
        result = conn.execute(select(table)).mappings().all()

    result_dicts = []
    for row in result:
        row_dict = {}
        for key, value in row.items():
            # Convert date/time to string
            if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
                row_dict[key] = value.isoformat()
            else:
                row_dict[key] = value
        result_dicts.append(row_dict)

    return jsonify(result_dicts)

# UPDATE
@app.route("/<table_name>/<int:pk>", methods=["PUT"])
def update_record(table_name, pk):
    table = tables.get(table_name)
    if table is None:
        return jsonify({"error": "Table not found"}), 404
    data = request.json
    pk_column = list(table.primary_key.columns)[0]
    with engine.begin() as conn:
        stmt = update(table).where(pk_column == pk).values(data)
        conn.execute(stmt)
    return jsonify({"status": "updated"})

# DELETE
@app.route("/<table_name>/<record_id>", methods=["DELETE"])
def delete_record(table_name, record_id):
    table = tables.get(table_name.upper())
    if table is None:
        return jsonify({"error": f"Table '{table_name}' not found"}), 404

    pk_column = list(table.primary_key.columns)[0]  # get primary key column
    pk_type = pk_column.type.python_type  # correct way

    # Convert record_id to the correct type
    try:
        if pk_type == int:
            record_id = int(record_id)
        elif pk_type == float:
            record_id = float(record_id)
        # leave string as is
    except ValueError:
        return jsonify({"error": f"Invalid {pk_column.name} value"}), 400

    stmt = table.delete().where(pk_column == record_id)
    try:
        with engine.begin() as conn:  # begin() handles commit automatically
            result = conn.execute(stmt)
        if result.rowcount == 0:
            return jsonify({"error": "Record not found"}), 404
        return jsonify({"message": "Deleted successfully"})
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500







def main():
    menu = """
Select an operation by number:
1. Create tables
2. Seed data
3. Update Arman phone
4. Update caregiver rates
5. Delete jobs posted by Amina Aminova
6. Delete members on Kabanbay Batyr street
7. Run 5.x SELECT queries
8. Run 6.x complex queries
9. Show total cost per caregiver
10. Show job applications view
0. Exit
"""
    while True:
        print(menu)
        choice = input("Enter choice: ").strip()
        if choice == "1":
            create_tables()
        elif choice == "2":
            seed_data()
        elif choice == "3":
            update_arman_phone()
        elif choice == "4":
            update_caregiver_rates(round_to_2decimals=True)
        elif choice == "5":
            delete_jobs_by_amina()
        elif choice == "6":
            delete_members_on_kabanbay()
        elif choice == "7":
            result = selects_5_x()
            print("5.x SELECT query results:")
            for k, v in result.items():
                print(f"{k}: {v}")
        elif choice == "8":
            result = queries_6_x()
            print("6.x complex query results:")
            for k, v in result.items():
                print(f"{k}: {v}")
        elif choice == "9":
            result = total_cost_per_caregiver()
            print("Total cost per caregiver:")
            for r in result:
                print(r)
        elif choice == "10":
            result = view_job_applications()
            print("Job applications view:")
            for r in result:
                print(r)
        elif choice == "0":
            print("Exiting.")
            break
        else:
            print("Invalid choice. Try again.")

import threading

def start_flask():
    app.run(debug=False)

if __name__ == "__main__":
    
    threading.Thread(target=start_flask, daemon=False).start()
    main()