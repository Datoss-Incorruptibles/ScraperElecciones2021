import insert_tables as it
import schedule
import time
import datetime
import shutil
from multiprocessing import Process

def inserData():
  print(f'insert data to DB  START at :{datetime.datetime.now()}')
  if __name__ == '__main__':
    p1 = Process(target=it.insertExpedientesLista)

    p2 = Process(target=it.insertGetCandidatos)

    p3 = Process(target=it.insertInfoPersonal)
    p4 = Process(target=it.insertEduBasic)
    p5 = Process(target=it.insertEduNoUni)
    p6 = Process(target=it.insertEduTecnica)
    p7 = Process(target=it.insertEduUni)

    p8 = Process(target=it.insertEduPostGrado)

    p9 = Process(target=it.insertExpLab)

    p10 = Process(target=it.insertSentCivil)
    p11 = Process(target=it.insertSentPenal)
    
    p12 = Process(target=it.insertCargoEleccion)
    p13 = Process(target=it.insertCargoPartidario)

    p14 = Process(target=it.insertInfoAdicional)

    p15 = Process(target=it.insertIngresos)
    p16 = Process(target=it.insertBienMueble)
    p17 = Process(target=it.insertBienInmueble)

    p18 = Process(target=it.insertPlanDeGobierno)
    p19 = Process(target=it.insertPlanDeGobiernoDimensiones)


    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    p10.start()
    p11.start()
    p12.start()
    p13.start()

    p14.start()
    p15.start()
    p16.start()
    p17.start()
    p18.start()
    p19.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()
    p10.join()
    p11.join()
    p12.join()
    p13.join()
    p14.join()
    p15.join()
    p16.join()
    p17.join()
    p18.join()
    p19.join()

  print(f'insert data to DB END at :{datetime.datetime.now()}')




def moveData():
  try:
    print(f'MOVE data STARTS at :{datetime.datetime.now()}')

    now = datetime.datetime.now()
    cleanTime = now.strftime("%Y-%m-%d %H:%M:%S")

    shutil.move("../currentRawData/GetExpedientesLista.json", f'../dataHistory/GetExpedientesLista{cleanTime}_.json')
    shutil.move("../currentRawData/GetCandidatos.json", f'../dataHistory/GetCandidatos{cleanTime}_.json')
    shutil.move("../currentRawData/CandidatoDatosHV.json", f'../dataHistory/CandidatoDatosHV{cleanTime}_.json')

    print(f'MOVE data ENDS at :{datetime.datetime.now()}')
  except:
    print(f'There are no files to move yet')


# def testData():
#   for i in range(1000):
#     print(i)

def job():
  print("Job start .... multiprocessos")
  inserData()
  moveData()
  print("Job end .... multiprocessos")

job()

# schedule.every(15).minutes.at("55:00").do(job)
# schedule.every().hour.at(":48").do(job)
# # schedule.every().day.at("02:30").do(job)
# schedule.every().minute.at(":50").do(job)

# while True:
#   schedule.run_pending()
#   time.sleep(1)