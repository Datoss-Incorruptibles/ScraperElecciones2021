import csv
import json
import datetime
import time
today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def convertGetExpedientesLista():
  with open(f'./currentRawData/GetExpedientesLista.json', 'r', encoding='utf-8') as outFile:
    doc = outFile.read()
    docString = json.loads(doc)

  with open(f'./currentRawData/GetExpedientesLista.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(["idOrganizacionPolitica",
                    "idExpediente",
                    "idJuradoUbicacion",
                    "idSolicitudLista",
                    "idTipoEleccion",
                    "strCarpeta",
                    "strCodExpediente",
                    "strDistritoElec",
                    "strEstadoLista",
                    "strJuradoElectoral",
                    "strOrganizacionPolitica",
                    "strRegion",
                    "strTipoOrganizacion",
                    "strUbigeo",
                    "idPlanGobierno",
                    "idProcesoElectoral",
                    "fecha_registro"])
    for row in docString:
      # print(row)
      writer.writerow([ 
                row["idOrganizacionPolitica"],
                row["idExpediente"],
                row["idJuradoUbicacion"],
                row["idSolicitudLista"],
                row["idTipoEleccion"],
                row["strCarpeta"],
                row["strCodExpediente"],
                row["strDistritoElec"],
                row["strEstadoLista"],
                row["strJuradoElectoral"],
                row["strOrganizacionPolitica"],
                row["strRegion"],
                row["strTipoOrganizacion"],
                row["strUbigeo"],
                row["idPlanGobierno"],
                row["idProcesoElectoral"],
                today
                ])




def convertGetCandidatos():
  with open(f'./currentRawData/GetCandidatos.json', 'r', encoding='utf-8') as outFile:
    doc = outFile.read()
    docString = json.loads(doc)

  with open(f'./currentRawData/GetCandidatos.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
                "idCandidato",
                "strDocumentoIdentidad",
                "idHojaVida",
                "idSolicitudLista",
                "intPosicion",
                "idCargoEleccion",
                "idExpediente",
                "idEstado",
                "strCargoEleccion",
                "strCandidato",
                "strOrganizacionPolitica",
                "idOrganizacionPolitica",
                "strUbigeoPostula",
                "strUbiDistritoPostula",
                "strUbiProvinciaPostula",
                "strUbiRegionPostula",
                "strEstadoExp",
                "idProcesoElectoral",
                "fecha_registro"])

    for row in docString:
      writer.writerow([ 
              row["idCandidato"],
              row["strDocumentoIdentidad"],
              row["idHojaVida"],
              row["idSolicitudLista"],
              row["intPosicion"],
              row["idCargoEleccion"],
              row["idExpediente"],
              row["idEstado"],
              row["strCargoEleccion"],
              row["strCandidato"],
              row["strOrganizacionPolitica"],
              row["idOrganizacionPolitica"],
              row["strUbigeoPostula"],
              row["strUbiDistritoPostula"],
              row["strUbiProvinciaPostula"],
              row["strUbiRegionPostula"],
              row["strEstadoExp"],
              row["idProcesoElectoral"],
                today])



            
# ----------------------------------------------------------------


def insertInfoPersonal():
  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0
  with open(f'./currentRawData/candidato_info_personal.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
                "strDocumentoIdentidad",
                "idCandidato",
                "idCargoEleccion",
                "idEstado",
                "idHojaVida",
                "idOrganizacionPolitica",
                "idParamHojaVida",
                "idProcesoElectoral",
                "idTipoEleccion",
                "strAnioPostula",
                "strApellidoMaterno",
                "strApellidoPaterno",
                "strCargoEleccion",
                "strCarneExtranjeria",
                "strClase",
                "strDomiDepartamento",
                "strDomiDistrito",
                "strDomiProvincia",
                "strDomicilioDirecc",
                "strEstado",
                "strFeTerminoRegistro",
                "strFechaNacimiento",
                "strHojaVida",
                "strNaciDepartamento",
                "strNaciDistrito",
                "strNaciProvincia",
                "strNombres",
                "strPaisNacimiento",
                "strPostulaDepartamento",
                "strPostulaDistrito",
                "strPostulaProvincia",
                "strProcesoElectoral",
                "strRutaArchivo",
                "strSexo",
                "strUbigeoDomicilio",
                "strUbigeoNacimiento",
                "strUbigeoPostula",
                "strUsuario",
                "fecha_registro"
                ])

    for row in arrayCandidatos:
      row = row["oDatosPersonales"]
      writer.writerow([
          row["strDocumentoIdentidad"],
          row["idCandidato"],
          row["idCargoEleccion"],
          row["idEstado"],
          row["idHojaVida"],
          row["idOrganizacionPolitica"],
          row["idParamHojaVida"],
          row["idProcesoElectoral"],
          row["idTipoEleccion"],
          row["strAnioPostula"],
          row["strApellidoMaterno"],
          row["strApellidoPaterno"],
          row["strCargoEleccion"],
          row["strCarneExtranjeria"],
          row["strClase"],
          row["strDomiDepartamento"],
          row["strDomiDistrito"],
          row["strDomiProvincia"],
          row["strDomicilioDirecc"],
          row["strEstado"],
          row["strFeTerminoRegistro"],
          row["strFechaNacimiento"],
          row["strHojaVida"],
          row["strNaciDepartamento"],
          row["strNaciDistrito"],
          row["strNaciProvincia"],
          row["strNombres"],
          row["strPaisNacimiento"],
          row["strPostulaDepartamento"],
          row["strPostulaDistrito"],
          row["strPostulaProvincia"],
          row["strProcesoElectoral"],
          row["strRutaArchivo"],
          row["strSexo"],
          row["strUbigeoDomicilio"],
          row["strUbigeoNacimiento"],
          row["strUbigeoPostula"],
          row["strUsuario"],
          today
          ])
  print(f'END inserting to candidato_info_personal at:{datetime.datetime.now()}')



def insertExpLab():
  print(f'START inserting to candidato_exp_laboral at:{datetime.datetime.now()}')
  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0

  with open(f'./currentRawData/candidato_exp_laboral.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idEstado",
              "idHojaVida",
              "idHVExpeLaboral",
              "intItemExpeLaboral",
              "strAnioTrabajoDesde",
              "strAnioTrabajoHasta",
              "strCentroTrabajo",
              "strDireccionTrabajo",
              "strOcupacionProfesion",
              "strRucTrabajo",
              "strTengoExpeLaboral",
              "strTrabajoDepartamento",
              "strTrabajoDistrito",
              "strTrabajoPais",
              "strTrabajoProvincia",
              "strUbigeoTrabajo",
              "strUsuario",
              "fecha_registro"
                ])

    for objExp in arrayCandidatos:
      arrayExpLab = objExp["lExperienciaLaboral"]
      for row in arrayExpLab:
        writer.writerow([
            row["idEstado"],
            row["idHojaVida"],
            row["idHVExpeLaboral"],
            row["intItemExpeLaboral"],
            row["strAnioTrabajoDesde"],
            row["strAnioTrabajoHasta"],
            row["strCentroTrabajo"],
            row["strDireccionTrabajo"],
            row["strOcupacionProfesion"],
            row["strRucTrabajo"],
            row["strTengoExpeLaboral"],
            row["strTrabajoDepartamento"],
            row["strTrabajoDistrito"],
            row["strTrabajoPais"],
            row["strTrabajoProvincia"],
            row["strUbigeoTrabajo"],
            row["strUsuario"],
          today
            ])
  print(f'END inserting to candidato_exp_laboral at:{datetime.datetime.now()}')

def insertEduPostGrado():
  print(f'START inserting to candidato_post_grado at:{datetime.datetime.now()}')

  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0

  with open(f'./currentRawData/candidato_post_grado.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idEstado",
              "idHojaVida",
              "idHVPosgrado",
              "intItemPosgrado",
              "strAnioPosgrado",
              "strCenEstudioPosgrado",
              "strConcluidoPosgrado",
              "strEgresadoPosgrado",
              "strEsDoctor",
              "strEsMaestro",
              "strEspecialidadPosgrado",
              "strTengoPosgrado",
              "strUsuario",
              "strComentario"])

    for row in arrayCandidatos:
      row = row["oEduPosgrago"]
      writer.writerow([
            row["idEstado"],
            row["idHojaVida"],
            row["idHVPosgrado"],
            row["intItemPosgrado"],
            row["strAnioPosgrado"],
            row["strCenEstudioPosgrado"],
            row["strConcluidoPosgrado"],
            row["strEgresadoPosgrado"],
            row["strEsDoctor"],
            row["strEsMaestro"],
            row["strEspecialidadPosgrado"],
            row["strTengoPosgrado"],
            row["strUsuario"],
            row["strComentario"],
            today

          ])
  print(f'END inserting to candidato_exp_laboral at:{datetime.datetime.now()}')


def insertEduUni():
  print(f'START inserting to candidato_edu_uni at:{datetime.datetime.now()}')
  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0

  with open(f'./currentRawData/candidato_edu_uni.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idEstado",
              "idHojaVida",
              "idHVEduUniversitaria",
              "intItemEduUni",
              "strAnioBachiller",
              "strAnioTitulo",
              "strBachillerEduUni",
              "strCarreraUni",
              "strConcluidoEduUni",
              "strEduUniversitaria",
              "strEgresadoEduUni",
              "strMetodoAccion",
              "strOrder",
              "strTengoEduUniversitaria",
              "strTituloUni",
              "strUniversidad",
              "strUsuario",
              "strComentario",
              "fecha_registro"
              ])


    for obj in arrayCandidatos:
      objPosGrado = obj["oEduPosgrago"]
      arrayData = obj["lEduUniversitaria"]
      for row in arrayData:
        writer.writerow([
            row["idEstado"],
            objPosGrado["idHojaVida"],
            row["idHVEduUniversitaria"],
            row["intItemEduUni"],
            row["strAnioBachiller"],
            row["strAnioTitulo"],
            row["strBachillerEduUni"],
            row["strCarreraUni"],
            row["strConcluidoEduUni"],
            row["strEduUniversitaria"],
            row["strEgresadoEduUni"],
            row["strMetodoAccion"],
            row["strOrder"],
            row["strTengoEduUniversitaria"],
            row["strTituloUni"],
            row["strUniversidad"],
            row["strUsuario"],
            row["strComentario"],
            today
            ])
  print(f'END inserting to candidato_edu_uni at:{datetime.datetime.now()}')



def insertEduTecnica():
  print(f'START inserting to candidato_edu_tecnica at:{datetime.datetime.now()}')

  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))

  with open(f'./currentRawData/candidato_edu_tecnica.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
                "idEstado",
                "idHojaVida",
                "idHVEduTecnico",
                "strCarreraTecnico",
                "strCenEstudioTecnico",
                "strConcluidoEduTecnico",
                "strTengoEduTecnico",
                "strUsuario",
                "strComentario",
                "fecha_registro"
              ])

    for row in arrayCandidatos:
        row = row["oEduTecnico"]
        if row:
          writer.writerow([
              row["idEstado"],
              row["idHojaVida"],
              row["idHVEduTecnico"],
              row["strCarreraTecnico"],
              row["strCenEstudioTecnico"],
              row["strConcluidoEduTecnico"],
              row["strTengoEduTecnico"],
              row["strUsuario"],
              row["strComentario"],
            today
              ])
  print(f'END inserting to candidato_edu_tecnica at:{datetime.datetime.now()}')


def insertEduNoUni():
  print(f'START inserting to candidato_edu_no_uni at:{datetime.datetime.now()}')

  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      
  with open(f'./currentRawData/candidato_edu_no_uni.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idEstado",
              "idHVNoUniversitaria",
              "idHojaVida",
              "strCarreraNoUni",
              "strCentroEstudioNoUni",
              "strConcluidoNoUni",
              "strTengoNoUniversitaria",
              "strUsuario",
              "fecha_registro"
              ])


    for row in arrayCandidatos:
        row = row["oEduNoUniversitaria"]
        writer.writerow([
            row["idEstado"],
            row["idHVNoUniversitaria"],
            row["idHojaVida"],
            row["strCarreraNoUni"],
            row["strCentroEstudioNoUni"],
            row["strConcluidoNoUni"],
            row["strTengoNoUniversitaria"],
            row["strUsuario"],
            today
        ])

  print(f'END inserting to candidato_edu_no_uni at:{datetime.datetime.now()}')


def insertEduBasic():
  print(f'START inserting to candidato_edu_basic at:{datetime.datetime.now()}')

  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      
  with open(f'./currentRawData/candidato_edu_basic.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idEstado",
              "idHVEduBasica",
              "idHojaVida",
              "strConcluidoEduPrimaria",
              "strConcluidoEduSecundaria",
              "strEduPrimaria",
              "strEduSecundaria",
              "strTengoEduBasica",
              "strUsuario",
              "fecha_registro"
              ])

    for row in arrayCandidatos:
        row = row["oEduBasica"]
        writer.writerow([
            row["idEstado"],
            row["idHVEduBasica"],
            row["idHojaVida"],
            row["strConcluidoEduPrimaria"],
            row["strConcluidoEduSecundaria"],
            row["strEduPrimaria"],
            row["strEduSecundaria"],
            row["strTengoEduBasica"],
            row["strUsuario"],
            today
          ])

  print(f'END inserting to candidato_edu_basic at:{datetime.datetime.now()}')



def insertSentPenal():

  print(f'START inserting to candidato_sent_penal at:{datetime.datetime.now()}')

  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))

  with open(f'./currentRawData/candidato_sent_penal.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idEstado",
              "idHVSentenciaPenal",
              "idHojaVida",
              "idParamCumpleFallo",
              "idParamModalidad",
              "intItemSentenciaPenal",
              "strCumpleFallo",
              "strDelitoPenal",
              "strExpedientePenal",
              "strFalloPenal",
              "strFechaSentenciaPenal",
              "strModalidad",
              "strOrder",
              "strOrganoJudiPenal",
              "strOtraModalidad",
              "strTengoSentenciaPenal",
              "strUsuario",
              "fecha_registro"
              ])

    for obj in arrayCandidatos:
      listSentPenal = obj["lSentenciaPenal"]
      for row in listSentPenal:
        writer.writerow([
                row["idEstado"],
                row["idHVSentenciaPenal"],
                row["idHojaVida"],
                row["idParamCumpleFallo"],
                row["idParamModalidad"],
                row["intItemSentenciaPenal"],
                row["strCumpleFallo"],
                row["strDelitoPenal"],
                row["strExpedientePenal"],
                row["strFalloPenal"],
                row["strFechaSentenciaPenal"],
                row["strModalidad"],
                row["strOrder"],
                row["strOrganoJudiPenal"],
                row["strOtraModalidad"],
                row["strTengoSentenciaPenal"],
                row["strUsuario"],
            today
        ])

  print(f'END inserting to candidato_sent_penal at:{datetime.datetime.now()}')


def insertSentCivil():
  print(f'START inserting to candidato_sent_civil at:{datetime.datetime.now()}')

  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))

  with open(f'./currentRawData/candidato_sent_civil.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idEstado",
              "idHVSentenciaObliga",
              "idHojaVida",
              "idParamMateriaSentencia",
              "intItemSentenciaObliga",
              "strEstado",
              "strExpedienteObliga",
              "strFalloObliga",
              "strMateriaSentencia",
              "strOrder",
              "strOrganoJuridicialObliga",
              "strTengoSentenciaObliga",
              "strUsuario",
              "fecha_registro"
              ])
    for obj in arrayCandidatos:
      listSentObl = obj["lSentenciaObliga"]
      for row in listSentObl:
        writer.writerow([
            row["idEstado"],
            row["idHVSentenciaObliga"],
            row["idHojaVida"],
            row["idParamMateriaSentencia"],
            row["intItemSentenciaObliga"],
            row["strEstado"],
            row["strExpedienteObliga"],
            row["strFalloObliga"],
            row["strMateriaSentencia"],
            row["strOrder"],
            row["strOrganoJuridicialObliga"],
            row["strTengoSentenciaObliga"],
            row["strUsuario"],
            today
        ])
  print(f'END inserting to candidato_sent_civil at:{datetime.datetime.now()}')

def insertCargoPartidario():
  print(f'START inserting to candidato_cargo_partidario at:{datetime.datetime.now()}')

  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0

  with open(f'./currentRawData/candidato_cargo_partidario.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idEstado",
              "idHVCargoPartidario",
              "idHojaVida",
              "idOrgPolCargoPartidario",
              "intItemCargoPartidario",
              "strAnioCargoPartiDesde",
              "strAnioCargoPartiHasta",
              "strCargoPartidario",
              "strOrder",
              "strOrgPolCargoPartidario",
              "strTengoCargoPartidario",
              "strComentario",
              "fecha_registro"
              ])


    for obj in arrayCandidatos:
      listCargoPart = obj["lCargoPartidario"]
      for row in listCargoPart:
        writer.writerow([
            row["idEstado"],
            row["idHVCargoPartidario"],
            row["idHojaVida"],
            row["idOrgPolCargoPartidario"],
            row["intItemCargoPartidario"],
            row["strAnioCargoPartiDesde"],
            row["strAnioCargoPartiHasta"],
            row["strCargoPartidario"],
            row["strOrder"],
            row["strOrgPolCargoPartidario"],
            row["strTengoCargoPartidario"],
            row["strComentario"],
            today
        ])
  print(f'END inserting to candidato_cargo_partidario at:{datetime.datetime.now()}')



def insertCargoEleccion():
  print(f'START inserting to candidato_cargo_eleccion at:{datetime.datetime.now()}')

  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0

  with open(f'./currentRawData/candidato_cargo_eleccion.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idCargoEleccion",
              "idEstado",
              "idHVCargoEleccion",
              "idHojaVida",
              "idOrgPolCargoElec",
              "intItemCargoEleccion",
              "strAnioCargoElecDesde",
              "strAnioCargoElecHasta",
              "strCargoEleccion",
              "strCargoEleccion2",
              "strOrder",
              "strOrgPolCargoElec",
              "strComentario",
              "fecha_registro"
              ])

    for obj in arrayCandidatos:
      listCargoElecc = obj["lCargoEleccion"]
      for row in listCargoElecc:
        writer.writerow([
            row["idCargoEleccion"],
            row["idEstado"],
            row["idHVCargoEleccion"],
            row["idHojaVida"],
            row["idOrgPolCargoElec"],
            row["intItemCargoEleccion"],
            row["strAnioCargoElecDesde"],
            row["strAnioCargoElecHasta"],
            row["strCargoEleccion"],
            row["strCargoEleccion2"],
            row["strOrder"],
            row["strOrgPolCargoElec"],
            row["strComentario"],
            today
        ])

  print(f'END inserting to candidato_cargo_eleccion at:{datetime.datetime.now()}')


def insertInfoAdicional():
  print(f'START inserting to candidato_info_adicional at:{datetime.datetime.now()}')

  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0

  with open(f'./currentRawData/candidato_info_adicional.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idEstado",
              "idHVInfoAdicional",
              "idHojaVida",
              "strInfoAdicional",
              "strTengoInfoAdicional",
              "strUsuario",
              "fecha_registro"
              ])


    for obj in arrayCandidatos:
        row = obj["oInfoAdicional"]
        writer.writerow([
            int(row["idEstado"]),
            row["idHVInfoAdicional"],
            row["idHojaVida"],
            row["strInfoAdicional"],
            row["strTengoInfoAdicional"],
            row["strUsuario"],
            today
        ])
  print(f'END inserting to candidato_info_adicional at:{datetime.datetime.now()}')


def insertIngresos():
  print(f'START inserting to candidato_ingresos at:{datetime.datetime.now()}')

  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))

  with open(f'./currentRawData/candidato_ingresos.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "decOtroIngresoPrivado",
              "decOtroIngresoPublico",
              "decRemuBrutaPrivado",
              "decRemuBrutaPublico",
              "decRentaIndividualPrivado",
              "decRentaIndividualPublico",
              "idEstado",
              "idHVIngresos",
              "idHojaVida",
              "strAnioIngresos",
              "strTengoIngresos",
              "strUsuario",
              "fecha_registro"
              ])
    for obj in arrayCandidatos:
        row = obj["oIngresos"]
        writer.writerow([
            row["decOtroIngresoPrivado"],
            row["decOtroIngresoPublico"],
            row["decRemuBrutaPrivado"],
            row["decRemuBrutaPublico"],
            row["decRentaIndividualPrivado"],
            row["decRentaIndividualPublico"],
            row["idEstado"],
            row["idHVIngresos"],
            row["idHojaVida"],
            row["strAnioIngresos"],
            row["strTengoIngresos"],
            row["strUsuario"],
            today
        ])

  print(f'END inserting to candidato_ingresos at:{datetime.datetime.now()}')


def insertBienMueble():
  print(f'START inserting to candidato_bien_mueble at:{datetime.datetime.now()}')
  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))

  with open(f'./currentRawData/candidato_bien_mueble.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "decValor",
              "idEstado",
              "idHVBienMueble",
              "idHojaVida",
              "intItemMueble",
              "strAnio",
              "strCaracteristica",
              "strComentario",
              "strMarca",
              "strModelo",
              "strOrder",
              "strPlaca",
              "strTengoBienMueble",
              "strUsuario",
              "strVehiculo",
              "fecha_registro"
              ])
    for obj in arrayCandidatos:
      listArray = obj["lBienMueble"]
      for row in listArray:         
        writer.writerow([
            row["decValor"],
            row["idEstado"],
            row["idHVBienMueble"],
            row["idHojaVida"],
            row["intItemMueble"],
            row["strAnio"],
            row["strCaracteristica"],
            row["strComentario"],
            row["strMarca"],
            row["strModelo"],
            row["strOrder"],
            row["strPlaca"],
            row["strTengoBienMueble"],
            row["strUsuario"],
            row["strVehiculo"],
            today
        ])

  print(f'END inserting to candidato_bien_mueble at:{datetime.datetime.now()}')


def insertBienInmueble():
  print(f'START inserting to candidato_bien_inmueble at:{datetime.datetime.now()}')
  with open('./currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))

  with open(f'./currentRawData/candidato_bien_inmueble.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "decAutovaluo",
              "decUIT",
              "idEstado",
              "idHVBienInmueble",
              "idHojaVida",
              "intItemInmueble",
              "strComentario",
              "strFichaTomoSunarp",
              "strInmuebleDepartamento",
              "strInmuebleDireccion",
              "strInmuebleDistrito",
              "strInmueblePais",
              "strInmuebleProvincia",
              "strInmuebleSunarp",
              "strInmuebleUbiDepartamento",
              "strInmuebleUbiDistrito",
              "strInmuebleUbiProvincia",
              "strOrder",
              "strPartidaSunarp",
              "strTengoInmueble",
              "strTipoBienInmueble",
              "strUbigeoInmueble",
              "strUsuario",
              "fecha_registro"
              ])
    for obj in arrayCandidatos:
      listArray = obj["lBienInmueble"]
      for row in listArray:  
        writer.writerow([
            row["decAutovaluo"],
            row["decUIT"],
            row["idEstado"],
            row["idHVBienInmueble"],
            row["idHojaVida"],
            row["intItemInmueble"],
            row["strComentario"],
            row["strFichaTomoSunarp"],
            row["strInmuebleDepartamento"],
            row["strInmuebleDireccion"],
            row["strInmuebleDistrito"],
            row["strInmueblePais"],
            row["strInmuebleProvincia"],
            row["strInmuebleSunarp"],
            row["strInmuebleUbiDepartamento"],
            row["strInmuebleUbiDistrito"],
            row["strInmuebleUbiProvincia"],
            row["strOrder"],
            row["strPartidaSunarp"],
            row["strTengoInmueble"],
            row["strTipoBienInmueble"],
            row["strUbigeoInmueble"],
            row["strUsuario"],
            today
            ])
  print(f'END inserting to candidato_bien_inmueble at:{datetime.datetime.now()}')

# ----------------------------------------------------------------
def org_poli_plan_gobierno():

  print(f'START inserting to org_poli_plan_gobierno at:{datetime.datetime.now()}')

  with open('./currentRawData/PlanesDeGobierno.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayPlanes = json.loads(str(doc))
    count = 0
  with open(f'./currentRawData/org_poli_plan_gobierno.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
                "idPlanGobierno",
                "idOrganizacionPolitica",
                "idProcesoElectoral",
                "strUbigeo",
                "idTipoEleccion",
                "strCodigoRegistroPG",
                "strTieneArchivo",
                "strFechaRegistro",
                "strUsuario",
                "idEstado",
                "intPorcentaje",
                "strCompleto",
                "idParamPlanGob",
                "strOrganizacionPolitica",
                "strTipoEleccion",
                "strEstado",
                "strDepartamento",
                "strProvincia",
                "strDistrito",
                "strDescripcionUbigeo",
                "strParamPlanGob",
                "strArchivo",
                "strRutaArchivo",
                "strExisteArchivoFisico",
                "strPlanGobiernoExistente",
                "strFechaResumenGenerado",
                "fecha_registro"
                ])

    for row in arrayPlanes:  
      if row is not None:
        writer.writerow([ 
                row["idPlanGobierno"],
                  row["idOrganizacionPolitica"],
                  row["idProcesoElectoral"],
                  row["strUbigeo"],
                  row["idTipoEleccion"],
                  row["strCodigoRegistroPG"],
                  row["strTieneArchivo"],
                  row["strFechaRegistro"],
                  row["strUsuario"],
                  row["idEstado"],
                  row["intPorcentaje"],
                  row["strCompleto"],
                  row["idParamPlanGob"],
                  row["strOrganizacionPolitica"],
                  row["strTipoEleccion"],
                  row["strEstado"],
                  row["strDepartamento"],
                  row["strProvincia"],
                  row["strDistrito"],
                  row["strDescripcionUbigeo"],
                  row["strParamPlanGob"],
                  row["strArchivo"],
                  row["strRutaArchivo"],
                  row["strExisteArchivoFisico"],
                  row["strPlanGobiernoExistente"],
                  row["strFechaResumenGenerado"],
            today
                ])


def org_poli_plan_gobierno_dimensiones():
  with open(f'./currentRawData/PlanesDeGobierno.json', 'r', encoding='utf-8') as outFile:
    doc = outFile.read()
    arrayPlanes = json.loads(str(doc))
    count = 0
    arrayOfPropuestas = []
    for plan in arrayPlanes:  
      if plan is not None:
        for row in plan["ListPGDSocial"]:  
          row["idPlanGobierno"] = plan["idPlanGobierno"]
          arrayOfPropuestas.append(row)
        for row in plan["ListPGDEconomica"]: 
          row["idPlanGobierno"] = plan["idPlanGobierno"]
          arrayOfPropuestas.append(row)
        for row in plan["ListPGDAmbiental"]:
          row["idPlanGobierno"] = plan["idPlanGobierno"]  
          arrayOfPropuestas.append(row)
        for row in plan["ListPGDInstitucional"]: 
          row["idPlanGobierno"] = plan["idPlanGobierno"] 
          arrayOfPropuestas.append(row)
        for row in plan["ListPTDPropuesta"]:  
          row["idPlanGobierno"] = plan["idPlanGobierno"] 
          arrayOfPropuestas.append(row)


  with open(f'./currentRawData/org_poli_plan_gobierno_dimensiones.csv', mode='w') as data:
    writer = csv.writer(data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
              "idPlanGobierno", 
              "idPlanGobDimension",
              "strPGProblema",
              "strPGObjetivo",
              "strPGMeta",
              "strPGIndicador",
              "strFechaRegistro",
              "strUsuario",
              "idEstado",
              "idPGDimension",
              "intPorcentaje",
              "fecha_registro"            
              ])

    for row in arrayOfPropuestas:
      writer.writerow([ 
              row["idPlanGobierno"],
              row["idPlanGobDimension"],
              row["strPGProblema"],
              row["strPGObjetivo"],
              row["strPGMeta"],
              row["strPGIndicador"],
              row["strFechaRegistro"],
              row["strUsuario"],
              row["idEstado"],
              row["idPGDimension"],
              row["intPorcentaje"],
            today
              ])





convertGetExpedientesLista()
convertGetCandidatos()

insertInfoPersonal()
insertExpLab()
insertEduPostGrado()
insertEduUni()
insertEduTecnica()
insertEduNoUni()
insertEduBasic()
insertSentPenal()
insertSentCivil()
insertCargoPartidario()
insertCargoEleccion()
insertInfoAdicional()
insertIngresos()
insertBienMueble()
insertBienInmueble()

org_poli_plan_gobierno()
org_poli_plan_gobierno_dimensiones()