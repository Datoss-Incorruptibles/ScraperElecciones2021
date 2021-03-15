import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv('../../.env')

con = psycopg2.connect(database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"), host=os.getenv("DB_HOST"), port="5432")
print(con)
print("Database opened successfully")


def cargaExpedientesLista():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.organizacion_politica_region;")
  copy_sql = """
            COPY jne.organizacion_politica_region FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/GetExpedientesLista.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table organizacion_politica_region cargado successfully")


def cargaGetCandidatos():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_info_electoral;")
  copy_sql = """
            COPY jne.candidato_info_electoral FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/GetCandidatos.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_info_electoral cargado successfully")



def cargaInfoPersonal():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_info_personal;")

  copy_sql = """
            COPY jne.candidato_info_personal
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_info_personal.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_info_personal cargado successfully")



def cargaExpLab():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_exp_laboral;")
  copy_sql = """
            COPY jne.candidato_exp_laboral FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_exp_laboral.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_exp_laboral cargado successfully")


def cargaEduPostGrado():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_post_grado;")
  copy_sql = """
            COPY jne.candidato_post_grado FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_post_grado.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_post_grado cargado successfully")

def cargaEduUni():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_edu_uni;")
  copy_sql = """
            COPY jne.candidato_edu_uni FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_edu_uni.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_edu_uni cargado successfully")

def cargaEduTecnica():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_edu_tecnica;")
  copy_sql = """
            COPY jne.candidato_edu_tecnica FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_edu_tecnica.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_edu_tecnica cargado successfully")

def cargaEduNoUni():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_edu_no_uni;")
  copy_sql = """
            COPY jne.candidato_edu_no_uni FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_edu_no_uni.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_edu_no_uni cargado successfully")

def cargaEduBasic():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_edu_basic;")
  copy_sql = """
            COPY jne.candidato_edu_basic FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_edu_basic.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_edu_basic cargado successfully")

def cargaSentPenal():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_sent_penal;")
  copy_sql = """
            COPY jne.candidato_sent_penal FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_sent_penal.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_sent_penal cargado successfully")

def cargaSentCivil():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_sent_civil;")
  copy_sql = """
            COPY jne.candidato_sent_civil FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_sent_civil.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_sent_civil cargado successfully")

def cargaCargoPartidario():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_cargo_partidario;")
  copy_sql = """
            COPY jne.candidato_cargo_partidario FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_cargo_partidario.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_cargo_partidario cargado successfully")

def cargaCargoEleccion():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_cargo_eleccion;")
  copy_sql = """
            COPY jne.candidato_cargo_eleccion FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_cargo_eleccion.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_cargo_eleccion cargado successfully")

def cargaInfoAdicional():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_info_adicional;")
  copy_sql = """
            COPY jne.candidato_info_adicional FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_info_adicional.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_info_adicional cargado successfully")

def cargaIngresos():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_ingresos;")
  copy_sql = """
            COPY jne.candidato_ingresos FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_ingresos.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_ingresos cargado successfully")

def cargaBienMueble():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_bien_mueble;")
  copy_sql = """
            COPY jne.candidato_bien_mueble FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_bien_mueble.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_bien_mueble cargado successfully")

def cargaBienInmueble():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.candidato_bien_inmueble;")
  copy_sql = """
            COPY jne.candidato_bien_inmueble FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/candidato_bien_inmueble.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table candidato_bien_inmueble cargado successfully")


def org_poli_plan_gobierno():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.org_poli_plan_gobierno;")
  copy_sql = """
            COPY jne.org_poli_plan_gobierno FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/org_poli_plan_gobierno.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table org_poli_plan_gobierno cargado successfully")


def org_poli_plan_gobierno_dimensiones():
  cur = con.cursor()
  cur.execute("TRUNCATE jne.org_poli_plan_gobierno_dimensiones;")
  copy_sql = """
            COPY jne.org_poli_plan_gobierno_dimensiones FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('../currentRawData/org_poli_plan_gobierno_dimensiones.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table org_poli_plan_gobierno_dimensiones cargado successfully")


cargaExpedientesLista()
cargaGetCandidatos()
cargaInfoPersonal()
cargaExpLab()
cargaEduPostGrado()
cargaEduUni()
cargaEduTecnica()
cargaEduNoUni()
cargaEduBasic()
cargaSentPenal()
cargaSentCivil()
cargaCargoPartidario()
cargaCargoEleccion()
cargaInfoAdicional()
cargaIngresos()
cargaBienMueble()
cargaBienInmueble()

org_poli_plan_gobierno()
org_poli_plan_gobierno_dimensiones()