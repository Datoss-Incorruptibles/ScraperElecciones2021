import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv('../../.env')

con = psycopg2.connect(database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"), host=os.getenv("DB_HOST"), port="5432")
print(con)
print("Database opened successfully")

# create tables 
def createOrgPoliticaRegion():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.organizacion_politica_region;
    CREATE TABLE IF NOT EXISTS jne.organizacion_politica_region(
        idOrganizacionPolitica int,
        idExpediente int,
        idJuradoUbicacion int,
        idSolicitudLista int,
        idTipoEleccion int,
        strCarpeta character varying,
        strCodExpediente character varying,
        strDistritoElec character varying,
        strEstadoLista character varying,
        strJuradoElectoral character varying,
        strOrganizacionPolitica character varying,
        strRegion character varying,
        strTipoOrganizacion character varying,
        strUbigeo character varying,
        idPlanGobierno int,
        idProcesoElectoral int,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table organizacion_politica_region created successfully")


def createInfoElectoral():

    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_info_electoral;

    CREATE TABLE IF NOT EXISTS jne.candidato_info_electoral (
        idCandidato int,
        strDocumentoIdentidad character varying,
        idHojaVida int,
        idSolicitudLista  int,
        intPosicion int,
        idCargoEleccion int,
        idExpediente int,
        idEstado int,
        strCargoEleccion character varying,
        strCandidato character varying,
        strOrganizacionPolitica character varying,
        idOrganizacionPolitica int,
        strUbigeoPostula character varying,
        strUbiDistritoPostula character varying,
        strUbiProvinciaPostula character varying,
        strUbiRegionPostula character varying,
        strEstadoExp character varying,
        idProcesoElectoral int,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_info_electoral created successfully")










def createInfoPersonal():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_info_personal;

    CREATE TABLE IF NOT EXISTS jne.candidato_info_personal (
        strDocumentoIdentidad character varying,
        idCandidato int,
        idCargoEleccion int,
        idEstado int,
        idHojaVida int,
        idOrganizacionPolitica int,
        idParamHojaVida int,
        idProcesoElectoral int,
        idTipoEleccion int,
        strAnioPostula character varying,
        strApellidoMaterno character varying,
        strApellidoPaterno character varying,
        strCargoEleccion character varying,
        strCarneExtranjeria character varying,
        strClase character varying,
        strDomiDepartamento character varying,
        strDomiDistrito character varying,
        strDomiProvincia character varying,
        strDomicilioDirecc character varying,
        strEstado character varying,
        strFeTerminoRegistro character varying,
        strFechaNacimiento character varying,
        strHojaVida character varying,
        strNaciDepartamento character varying,
        strNaciDistrito character varying,
        strNaciProvincia character varying,
        strNombres character varying,
        strPaisNacimiento character varying,
        strPostulaDepartamento character varying,
        strPostulaDistrito character varying,
        strPostulaProvincia character varying,
        strProcesoElectoral character varying,
        strRutaArchivo character varying,
        strSexo character varying,
        strUbigeoDomicilio character varying,
        strUbigeoNacimiento character varying,
        strUbigeoPostula character varying,
        strUsuario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_info_personal created successfully")




def createExpLab():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_exp_laboral;

    CREATE TABLE IF NOT EXISTS jne.candidato_exp_laboral (
        idEstado int,
        idHojaVida int,
        idHVExpeLaboral int,
        intItemExpeLaboral int,
        strAnioTrabajoDesde character varying,
        strAnioTrabajoHasta character varying,
        strCentroTrabajo character varying,
        strDireccionTrabajo character varying,
        strOcupacionProfesion character varying,
        strRucTrabajo character varying,
        strTengoExpeLaboral character varying,
        strTrabajoDepartamento character varying,
        strTrabajoDistrito character varying,
        strTrabajoPais character varying,
        strTrabajoProvincia character varying,
        strUbigeoTrabajo character varying,
        strUsuario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_exp_laboral created successfully")




def createEduPostGrado():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_post_grado;

    CREATE TABLE IF NOT EXISTS jne.candidato_post_grado (
        idEstado int,
        idHojaVida int,
        idHVPosgrado int,
        intItemPosgrado int,
        strAnioPosgrado character varying,
        strCenEstudioPosgrado character varying,
        strConcluidoPosgrado character varying,
        strEgresadoPosgrado character varying,
        strEsDoctor character varying,
        strEsMaestro character varying,
        strEspecialidadPosgrado character varying,
        strTengoPosgrado character varying,
        strUsuario character varying,
        strComentario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_post_grado created successfully")


def createEduUni():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_edu_uni;

    CREATE TABLE IF NOT EXISTS jne.candidato_edu_uni(
        idEstado int,
        idHojaVida int,
        idHVEduUniversitaria int,
        intItemEduUni int,
        strAnioBachiller character varying,
        strAnioTitulo character varying,
        strBachillerEduUni character varying,
        strCarreraUni character varying,
        strConcluidoEduUni character varying,
        strEduUniversitaria character varying,
        strEgresadoEduUni character varying,
        strMetodoAccion character varying,
        strOrder character varying,
        strTengoEduUniversitaria character varying,
        strTituloUni character varying,
        strUniversidad character varying,
        strUsuario character varying,
        strComentario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_edu_uni created successfully")



def createEduTecnica():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_edu_tecnica;

    CREATE TABLE IF NOT EXISTS jne.candidato_edu_tecnica(
        idEstado int,
        idHojaVida int,
        idHVEduTecnico int,
        strCarreraTecnico character varying,
        strCenEstudioTecnico character varying,
        strConcluidoEduTecnico character varying,
        strTengoEduTecnico character varying,
        strUsuario character varying,
        strComentario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_edu_tecnica created successfully")




def createEduNoUni():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_edu_no_uni;

    CREATE TABLE IF NOT EXISTS jne.candidato_edu_no_uni(
        idEstado int,
        idHVNoUniversitaria int,
        idHojaVida int,
        strCarreraNoUni character varying,
        strCentroEstudioNoUni character varying,
        strConcluidoNoUni character varying,
        strTengoNoUniversitaria character varying,
        strUsuario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_edu_no_uni created successfully")




def createEduBasic():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_edu_basic;

    CREATE TABLE IF NOT EXISTS jne.candidato_edu_basic(
        idEstado int,
        idHVEduBasica int,
        idHojaVida int,
        strConcluidoEduPrimaria character varying,
        strConcluidoEduSecundaria character varying,
        strEduPrimaria character varying,
        strEduSecundaria character varying,
        strTengoEduBasica character varying,
        strUsuario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_edu_basic created successfully")



def createSentPenal():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_sent_penal;

    CREATE TABLE IF NOT EXISTS jne.candidato_sent_penal(
        idEstado int,
        idHVSentenciaPenal int,
        idHojaVida int,
        idParamCumpleFallo int,
        idParamModalidad int,
        intItemSentenciaPenal int,
        strCumpleFallo character varying,
        strDelitoPenal character varying,
        strExpedientePenal character varying,
        strFalloPenal character varying,
        strFechaSentenciaPenal character varying,
        strModalidad character varying,
        strOrder character varying,
        strOrganoJudiPenal character varying,
        strOtraModalidad character varying,
        strTengoSentenciaPenal character varying,
        strUsuario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_sent_penal created successfully")



def createSentCivil():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_sent_civil;

    CREATE TABLE IF NOT EXISTS jne.candidato_sent_civil(
        idEstado int,
        idHVSentenciaObliga int,
        idHojaVida int,
        idParamMateriaSentencia int,
        intItemSentenciaObliga int,
        strEstado character varying,
        strExpedienteObliga character varying,
        strFalloObliga character varying,
        strMateriaSentencia character varying,
        strOrder character varying,
        strOrganoJuridicialObliga character varying,
        strTengoSentenciaObliga character varying,
        strUsuario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_sent_civil created successfully")






def createCargoPartidario():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_cargo_partidario;

    CREATE TABLE IF NOT EXISTS jne.candidato_cargo_partidario(
        idEstado int,
        idHVCargoPartidario int,
        idHojaVida int,
        idOrgPolCargoPartidario int,
        intItemCargoPartidario int,
        strAnioCargoPartiDesde character varying,
        strAnioCargoPartiHasta character varying,
        strCargoPartidario character varying,
        strOrder character varying,
        strOrgPolCargoPartidario character varying,
        strTengoCargoPartidario character varying,
        strComentario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_cargo_partidario created successfully")



def createCargoEleccion():
    cur = con.cursor()
    cur.execute('''
    DROP TABLE IF EXISTS  jne.candidato_cargo_eleccion;

    CREATE TABLE IF NOT EXISTS jne.candidato_cargo_eleccion(
        idCargoEleccion int,
        idEstado int,
        idHVCargoEleccion int,
        idHojaVida int,
        idOrgPolCargoElec int,
        intItemCargoEleccion int,
        strAnioCargoElecDesde character varying,
        strAnioCargoElecHasta character varying,
        strCargoEleccion character varying,
        strCargoEleccion2 character varying,
        strOrder character varying,
        strOrgPolCargoElec character varying,
        strComentario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_cargo_eleccion created successfully")






def createInfoAdicional():
    cur = con.cursor()
    cur.execute('''
    drop table IF EXISTS jne.candidato_info_adicional;

    CREATE TABLE IF NOT EXISTS jne.candidato_info_adicional(
        idEstado int,
        idHVInfoAdicional int,
        idHojaVida int,
        strInfoAdicional character varying,
        strTengoInfoAdicional character varying,
        strUsuario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_info_adicional created successfully")





def createIngresos():
    cur = con.cursor()
    cur.execute('''
    drop table IF EXISTS jne.candidato_ingresos;

    CREATE TABLE IF NOT EXISTS jne.candidato_ingresos(
        decOtroIngresoPrivado decimal,
        decOtroIngresoPublico decimal,
        decRemuBrutaPrivado decimal,
        decRemuBrutaPublico decimal,
        decRentaIndividualPrivado decimal,
        decRentaIndividualPublico decimal,
        idEstado int,
        idHVIngresos int,
        idHojaVida int,
        strAnioIngresos character varying,
        strTengoIngresos character varying,
        strUsuario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_ingresos created successfully")


def createBienMueble():
    cur = con.cursor()
    cur.execute('''
    drop table IF EXISTS jne.candidato_bien_mueble;
    CREATE TABLE IF NOT EXISTS jne.candidato_bien_mueble(
        decValor decimal,
        idEstado int,
        idHVBienMueble int,
        idHojaVida int,
        intItemMueble int,
        strAnio character varying,
        strCaracteristica character varying,
        strComentario character varying,
        strMarca character varying,
        strModelo character varying,
        strOrder character varying,
        strPlaca character varying,
        strTengoBienMueble character varying,
        strUsuario character varying,
        strVehiculo character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table candidato_bien_mueble created successfully")



def createBienInMueble():
    cur = con.cursor()
    cur.execute('''

    drop table IF EXISTS jne.candidato_bien_inmueble;

    CREATE TABLE IF NOT EXISTS jne.candidato_bien_inmueble(
        decAutovaluo decimal,
        decUIT decimal,
        idEstado int,
        idHVBienInmueble int,
        idHojaVida int,
        intItemInmueble int,
        strComentario character varying,
        strFichaTomoSunarp character varying,
        strInmuebleDepartamento character varying,
        strInmuebleDireccion character varying,
        strInmuebleDistrito character varying,
        strInmueblePais character varying,
        strInmuebleProvincia character varying,
        strInmuebleSunarp character varying,
        strInmuebleUbiDepartamento character varying,
        strInmuebleUbiDistrito character varying,
        strInmuebleUbiProvincia character varying,
        strOrder character varying,
        strPartidaSunarp character varying,
        strTengoInmueble character varying,
        strTipoBienInmueble character varying,
        strUbigeoInmueble character varying,
        strUsuario character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL

        );
    ''')
    con.commit()
    print("Table candidato_bien_inmueble created successfully")




def createPlanDeGobierno():
    cur = con.cursor()
    cur.execute('''
    drop table IF EXISTS jne.org_poli_plan_gobierno;
    CREATE TABLE IF NOT EXISTS jne.org_poli_plan_gobierno(
        idPlanGobierno int,
        idOrganizacionPolitica int,
        idProcesoElectoral int,
        strUbigeo character varying,
        idTipoEleccion int,
        strCodigoRegistroPG character varying,
        strTieneArchivo character varying,
        strFechaRegistro character varying,
        strUsuario character varying,
        idEstado int,
        intPorcentaje int,
        strCompleto character varying,
        idParamPlanGob int,
        strOrganizacionPolitica character varying,
        strTipoEleccion  character varying,
        strEstado  character varying,
        strDepartamento  character varying,
        strProvincia  character varying,
        strDistrito  character varying,
        strDescripcionUbigeo  character varying,
        strParamPlanGob  character varying,
        strArchivo  character varying,
        strRutaArchivo  character varying,
        strExisteArchivoFisico  character varying,
        strPlanGobiernoExistente character varying,
        strFechaResumenGenerado  character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table org_poli_plan_gobierno created successfully")




def createPlanDeGobiernoDimensiones():
    cur = con.cursor()
    cur.execute('''
    drop table IF EXISTS jne.org_poli_plan_gobierno_dimensiones;
    CREATE TABLE IF NOT EXISTS jne.org_poli_plan_gobierno_dimensiones(
        idPlanGobierno  int,
        idPlanGobDimension  int,
        strPGProblema  character varying,
        strPGObjetivo  character varying,
        strPGMeta  character varying,
        strPGIndicador  character varying,
        strFechaRegistro  character varying,
        strUsuario  character varying,
        idEstado  int,
        idPGDimension  int,
        intPorcentaje  int,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table org_poli_plan_gobierno_dimensiones created successfully")




createOrgPoliticaRegion()
createInfoElectoral()
createInfoPersonal()
createExpLab()
createEduPostGrado()
createEduUni()
createEduTecnica()
createEduNoUni()
createEduBasic()
createSentPenal()
createSentCivil()
createCargoPartidario()
createCargoEleccion()
createInfoAdicional()
createIngresos()
createBienMueble()
createBienInMueble()
createPlanDeGobierno()
createPlanDeGobiernoDimensiones()

con.close()





















# aun no se IMPLEMENTARA

# cur = con.cursor()
# cur.execute('''
# CREATE TABLE IF NOT EXISTS procesos_electorales(
#     idEstado int,
#     idProcesoElectoral int,
#     idTipoProceso int,
#     intCantidadJee int,
#     strDocConvocatoria character varying,
#     strEstado character varying,
#     strFechaAperturaProceso character varying,
#     strFechaConvocatoria character varying,
#     strFechaCierreProceso character varying,
#     strFechaRegistro character varying,
#     strNombreArchivo character varying,
#     strProcesoElectoral character varying,
#     strSiglas character varying,
#     strTipoProceso character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table proceso created successfully")
