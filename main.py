from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional

from sqlalchemy import create_engine, Column, Integer, String, Float, select, func
from sqlalchemy.orm import declarative_base, sessionmaker

import pandas as pd

Base = declarative_base()

# =====================================================
#                     МОДЕЛЬ ДАННЫХ
# =====================================================
class Student(Base):
    __tablename__ = "students"

    id = Column(Integer, primary_key=True, autoincrement=True)
    last_name = Column(String, nullable=False)
    first_name = Column(String, nullable=False)
    faculty = Column(String, nullable=False)
    course = Column(String, nullable=False)
    grade = Column(Float, nullable=False)

    def __repr__(self):
        return f"<Student({self.last_name} {self.first_name}, {self.faculty}, {self.course}, {self.grade})>"


# =====================================================
#                     SCHEMAS Pydantic
# =====================================================
class StudentCreate(BaseModel):
    last_name: str
    first_name: str
    faculty: str
    course: str
    grade: float


class StudentUpdate(BaseModel):
    last_name: Optional[str] = None
    first_name: Optional[str] = None
    faculty: Optional[str] = None
    course: Optional[str] = None
    grade: Optional[float] = None


class StudentOut(BaseModel):
    id: int
    last_name: str
    first_name: str
    faculty: str
    course: str
    grade: float

    class Config:
        orm_mode = True


# =====================================================
#                  КЛАСС РАБОТЫ С БД
# =====================================================
class StudentDB:
    def __init__(self, db_url="sqlite:///students.db"):
        self.engine = create_engine(db_url, echo=False)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    # -------- Create из CSV --------
    def insert_from_csv(self, csv_path: str):
        df = pd.read_csv(csv_path)
        df.columns = ['last_name', 'first_name', 'faculty', 'course', 'grade']
        with self.Session() as session:
            for _, row in df.iterrows():
                student = Student(**row.to_dict())
                session.add(student)
            session.commit()

    # -------- CREATE --------
    def create_student(self, data: StudentCreate):
        with self.Session() as session:
            student = Student(**data.dict())
            session.add(student)
            session.commit()
            session.refresh(student)
            return student

    # -------- READ --------
    def get_all_students(self):
        with self.Session() as session:
            return session.scalars(select(Student)).all()

    def get_student(self, student_id: int):
        with self.Session() as session:
            return session.get(Student, student_id)

    # -------- UPDATE --------
    def update_student(self, student_id: int, data: StudentUpdate):
        with self.Session() as session:
            student = session.get(Student, student_id)
            if not student:
                return None

            for key, value in data.dict(exclude_unset=True).items():
                setattr(student, key, value)

            session.commit()
            session.refresh(student)
            return student

    # -------- DELETE --------
    def delete_student(self, student_id: int):
        with self.Session() as session:
            student = session.get(Student, student_id)
            if not student:
                return False
            session.delete(student)
            session.commit()
            return True

    # -------- Доп. методы (оставлены из прошлой версии) --------
    def get_students_by_faculty(self, faculty_name: str):
        with self.Session() as session:
            stmt = select(Student).where(Student.faculty == faculty_name)
            return session.scalars(stmt).all()

    def get_unique_courses(self):
        with self.Session() as session:
            stmt = select(Student.course).distinct()
            return [row[0] for row in session.execute(stmt)]

    def get_avg_grade_by_faculty(self, faculty_name: str):
        with self.Session() as session:
            stmt = select(func.avg(Student.grade)).where(Student.faculty == faculty_name)
            avg_grade = session.scalar(stmt)
            return round(avg_grade, 2) if avg_grade else None

    def get_students_with_low_grade(self, course_name: str):
        with self.Session() as session:
            stmt = select(Student).where(Student.course == course_name, Student.grade < 30)
            return session.scalars(stmt).all()


# =====================================================
#                     FASTAPI
# =====================================================
app = FastAPI(title="Students REST API")

db = StudentDB()


# ------------------ CREATE ------------------
@app.post("/students/", response_model=StudentOut)
def create_student(student: StudentCreate):
    return db.create_student(student)


# ------------------ READ ------------------
@app.get("/students/", response_model=List[StudentOut])
def get_students():
    return db.get_all_students()


@app.get("/students/{student_id}", response_model=StudentOut)
def get_student(student_id: int):
    student = db.get_student(student_id)
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    return student


# ------------------ UPDATE ------------------
@app.put("/students/{student_id}", response_model=StudentOut)
def update_student(student_id: int, data: StudentUpdate):
    student = db.update_student(student_id, data)
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    return student


# ------------------ DELETE ------------------
@app.delete("/students/{student_id}")
def delete_student(student_id: int):
    ok = db.delete_student(student_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Student not found")
    return {"status": "deleted"}
