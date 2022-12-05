#python:
import pandas as pd
from io import StringIO
import csv
import codecs
import json
from sklearn.preprocessing import OneHotEncoder
import joblib
import numpy as np


#Fastapi:
from fastapi import FastAPI
from fastapi import File,UploadFile
from fastapi.responses import FileResponse 
from fastapi import Form


#Preprocces:
moras=["MORA PERIODO 2009", "MORA PERIODO 2010","MORA PERIODO 2011","MORA PERIODO 2012","MORA PERIODO 2013","MORA PERIODO 2014","MORA PERIODO 2015","MORA PERIODO 2016","MORA PERIODO 2017","MORA PERIODO 2018","MORA PERIODO 2019","MORA PERIODO 2020"]
pagos=["PAGO 2009","PAGO 2010","PAGO 2011","PAGO 2012","PAGO 2013","PAGO 2014","PAGO 2015","PAGO 2016","PAGO 2017","PAGO 2018","PAGO 2019","PAGO 2020"]
años=[2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020]
actividades=["ALOJAMIENTO","CLUB","COMERCIO","CONSUMO DE LICOR","DISCOTECA","ENTRETENIMIENTO","RESTAURANTE","SALUD","SERVICIOS","TRANSPORTE","EDUCACION","RELIGIOSO","OTROS"]
one_va=["LOCALIDAD","ACTIVIDAD","GESTION COBRO","ESTADO JURIDICO","CONVENIO","ACTA"]
###

def MinMaxScaler(variable,data):
  for i in data.index:
    data[variable][i]=(data[variable][i]-data[variable].min())/(data[variable].max()-data[variable].min())
    
def full_money(pagos,moras,instancia,data):
  dinero_total=0
  for i in range(len(pagos)):
    dinero_total=dinero_total+data[pagos[i]][instancia]+data[moras[i]][instancia]
  return dinero_total


app=FastAPI()

@app.get("/")
def home():
    return {"BIOS":"GLOBAL"}

@app.post(
    path="/upload-csv/{modelo_a_ejecutar}"
)
def upload_csv(
    #csv:UploadFile=File(...),
    #modelo_a_ejecutar:str=Form(...,title="Modelo a Ejecutar",description="Escriba el modelo que desea ejecutar: nuevos clientes o antiguos clientes ")
    modelo_a_ejecutar:str
    
    
):
    
    modelo_a_ejecutar=modelo_a_ejecutar.lower()
    
    if modelo_a_ejecutar=="nuevos clientes":
        try:
            data=pd.read_csv(StringIO(str(csv.file.read(), 'latin-1')), encoding='latin-1',sep=";")
        except:
            data=pd.read_csv(StringIO(str(csv.file.read(), 'latin-1')), encoding='latin-1')
            
        devolucion=data.copy()
        NV=["TARIFA BASE"]    
        variables_en_cuenta=["ESTRATO","TARIFA BASE","CODIGO","CATEGORIA","LOCALIDAD","ACTIVIDAD","GESTION COBRO","ESTADO JURIDICO","CONVENIO","ACTA"]
        no_variables_en_cuenta=data.drop(variables_en_cuenta,axis=1)
        data=data[variables_en_cuenta]
        
        
        data["CODIGO"]=data["CODIGO"].astype(str)  
        A_vinculacion=[]
        for i in data.index:
            vinculacion=data["CODIGO"][i][:4]
            A_vinculacion.append(vinculacion)
        data["CODIGO"]=data["CODIGO"].astype(int)
        data["ANO VINCULACION"]=A_vinculacion
        data["ANO VINCULACION"]=data["ANO VINCULACION"].astype(int)
        devolucion["ANO VINCULACION"]=data["ANO VINCULACION"]
        
        
        data["TARIFA BASE"]=data["TARIFA BASE"].astype(str)    
        for i in data.index:
            string=data["TARIFA BASE"][i]
            string = ''.join( x for x in string if x not in "$.,")
            data["TARIFA BASE"][i]=string
        data["TARIFA BASE"] = data["TARIFA BASE"].astype(int, errors = 'raise') 
        
        for i in data.index:
            actividad=data["ACTIVIDAD"][i]
            if (actividad[:5]=="HOTEL" or actividad[:9]=="BALNEARIO" or actividad[:9]=="HOSPEDAJE" or actividad[:8]=="ESTADERO" or actividad[:5]=="MOTEL" or actividad[:11]=="RESIDENCIAS" or actividad[:6]=="HOSTAL"):
                data["ACTIVIDAD"][i]="ALOJAMIENTO"

            elif (actividad[:9]=="CENTRO DE" or actividad[:3]=="CLU"):
                data["ACTIVIDAD"][i]="CLUB"

            elif (actividad[:7]=="ALMACEN" or actividad[:6]=="INTERN" or actividad[len(actividad)-8:len(actividad)]=="INTERNET" or actividad[:5]=="CABIN" or actividad[:5]=="VENTA" or actividad[:6]=="SASTRE" or actividad[:6]=="REMONT" or actividad[:8]=="SERVICIO" or actividad[:4]=="CATE"  or actividad[len(actividad)-8:len(actividad)]=="GASOLINA" or  actividad=="SALOPN DE RECEPCIONES" or actividad[:5]=="GASOL" or actividad[:3]=="SPA" or actividad[:6]=="PORTAL" or actividad[:4]=="LAVA" or actividad[:3]=="AGE" or actividad[:5]=="SAUNA" or actividad[:6]=="MANTEN" or actividad[:3]=="WEB" or actividad[:5]=="DISEN" or  actividad[:4]=="AUTO" or actividad[:8]=="COMUNICA" or actividad[:6]=="ACTIVI" or actividad[:5]=="APUES" or actividad[:6]=="PUBLIC" or actividad[:4]=="DROG" or actividad[:4]=="Drog" or actividad[:5]=="FARMA" or actividad[:3]=="ALM" or actividad[:3]=="VNT" or actividad[:7]=="NATURIS" or actividad[:5]=="ARTIC" or actividad[:6]=="CALZAD" or actividad[:7]=="GIGARRE" or actividad[:4]=="ALAM" or actividad[:7]=="CERAMIC" or actividad[:6]=="Tienda" or actividad[:7]=="COGARRE" or actividad[:5]=="ALMAC" or actividad[:5]=="Almac" or actividad[:6]=="TIENDA" or actividad[:3]=="CIG" or actividad[:5]=="SIGAR" or actividad[:7]=="cigarre" or actividad[len(actividad)-10:len(actividad)]=="CIGARRERIA" or actividad[:12]=="AUTOSERVICIO" or actividad[:3]=="VTA" or actividad[:8]=="LICORERA" or actividad[:3]=="FRU" or actividad[:9]=="PAPELERIA" or actividad[:7]=="CONSECI" or actividad[:13]=="CONCESIONARIO" or actividad[:4]=="JOYE" or actividad[:6]=="FERRET" or actividad[:7]=="CARNICE" or actividad[len(actividad)-10:len(actividad)]=="CARNICERIA" or actividad[:7]=="CACHARR" or actividad[:6]=="BICICL" or actividad[:5]=="VIDRI" or actividad[:6]=="MISCEL" or actividad[:5]=="OPTIC" or actividad[:5]=="DULCE" or actividad[:4]=="CASA" or actividad[:7]=="MINIMAR" or actividad[:7]=="MINIMER" or actividad[:6]=="CENTRO" or actividad[:13]=="DISTRIBUIDORA" or actividad[:6]=="Distri" or actividad[:7]=="ESTUDIO" or actividad[:11]=="GRIFORIFICO" or actividad[:7]=="GRANDES" or actividad[:6]=="KIOSCO" or actividad[:8]=="IMPRENTA" or actividad[:7]=="OFICINA" or actividad[:6]=="TIENDA" or actividad[:5]=="SUPER" or actividad[len(actividad)-7:len(actividad)]=="MERCADO" or actividad[:6]=="COMERC" or actividad[:11]=="CORPORACION"):
                data["ACTIVIDAD"][i]="COMERCIO"

            elif (actividad[:9]=="CAFETERIA" or actividad[:5]=="SALON" or actividad[:9]=="HELADERIA" or actividad[:3]=="TDA" or actividad[len(actividad)-5:len(actividad)]=="LICOR" or actividad[len(actividad)-8:len(actividad)]=="LICORERA" or actividad[:6]=="ROKOLA" or actividad[:4]=="ROCK" or actividad[len(actividad)-7:len(actividad)]=="CERVEZA" or actividad[len(actividad)-8:len(actividad)]=="BOLIRANA" or actividad[len(actividad)-7:len(actividad)]=="LICORES" or actividad[len(actividad)-8:len(actividad)]=="LICORES " or actividad[len(actividad)-3:len(actividad)]=="LUZ" or actividad[:6]=="EXPEND" or actividad[:5]=="LICOR"):
                data["ACTIVIDAD"][i]="CONSUMO DE LICOR"

            elif (actividad[:8]=="ESTADERO" or actividad[:5]=="DISCO" or actividad[:5]=="DISKO" or actividad[:5]=="Disco" or actividad[:4]=="Club" or actividad[:8]=="WISKERIA" or actividad[:3]=="BAR" or actividad[:4]==" BAR" or actividad[:3]=="bar" or actividad[len(actividad)-4:len(actividad)]==" BAR" or actividad[:7]=="TABERNA" or actividad[:5]=="FONDA"):
                data["ACTIVIDAD"][i]="DISCOTECA"

            elif (actividad[:6]=="TEATRO" or actividad[:4]=="SALA" or actividad[:4]=="jueg" or actividad[:4]=="CANH" or actividad[:4]=="ACAE" or actividad[:6]=="CASINO" or actividad[:5]=="BINGO" or actividad[:7]=="GALLERA" or actividad[:6]=="CACINO" or actividad[:10]=="CENTRO DEP" or actividad[:4]=="CINE" or actividad[:6]=="PARQUE" or actividad[:6]=="BILLAR" or actividad[len(actividad)-6:len(actividad)]=="BILLAR" or actividad[len(actividad)-6:len(actividad)]=="BILLAR" or actividad[:3]=="BOL" or actividad[:8]=="GIMNASIO" or actividad[:3]=="GYM" or actividad[:3]=="GIM" or actividad[:5]=="TRAIN" or actividad[:6]=="CANCHA" or actividad[:5]=="CAMPO" or actividad[:5]=="JUEGO" or actividad[:7]=="KARAOKE" or actividad[len(actividad)-4:len(actividad)]=="TEJO" or actividad[:4]=="RANO" or actividad[len(actividad)-6:len(actividad)==" BAILE"]):
                data["ACTIVIDAD"][i]="ENTRETENIMIENTO"

            elif (actividad[:9]=="CAFETERIA" or actividad[:11]=="RESTAURANTE" or actividad[:11]=="POLLE" or actividad[:5]=="PIZZE" or actividad[:7]=="PIQUETE" or actividad[:8]=="SALSAMEN" or actividad[:7]=="ASADERO" or actividad[:6]=="COMIDA" or actividad[:5]=="PANAD" or actividad[:4]=="CAFE" or actividad[len(actividad)-4:len(actividad)]=="CAFE") or actividad[:4]=="PAST":
                data["ACTIVIDAD"][i]="RESTAURANTE"

            elif (actividad[:10]=="CENTRO MED" or actividad[:7]=="CLINICA" or actividad[:7]=="Clinica"   or actividad[:11]=="CONSULTORIO" or actividad[:8]=="HOSPITAL" or actividad[:5]=="MEDIC" or actividad[len(actividad)-2:len(actividad)]=="PS"  or actividad[len(actividad)-6:len(actividad)]=="FISICO" or actividad[:6]=="ACONDI"):
                data["ACTIVIDAD"][i]="SALUD"

            elif (actividad[:12]=="CENTRO DE ES" or actividad[:7]=="ENTIDAD" or actividad[:5]=="SDALA" or actividad[:6]=="Taller" or actividad[:6]==" TALLER"    or actividad[:5]=="ESTET"   or actividad[:6]=="TALLER" or actividad[:4]=="PELU" or actividad[:8]=="ESTETICA" or actividad[:15]=="SALA DE BELLEZA" or actividad[len(actividad)-8:len(actividad)]=="Estetica"):
                data["ACTIVIDAD"][i]="SERVICIOS"

            elif (actividad[:8]=="TERMINAL" or actividad[:10]=="AEROPUERTO" or actividad[:14]=="TRANSPORTADORA"):
                data["ACTIVIDAD"][i]="TRANSPORTE"
            
            elif (actividad[:10]=="ENTIDAD ED" or actividad[:8]=="ACADEMIA" or actividad[len(actividad)-6:len(actividad)]=="JARDIN"):
                data["ACTIVIDAD"][i]="EDUCACION"
            
            elif (actividad[:7]=="IGLESIA"):
                data["ACTIVIDAD"][i]="RELIGIOSO"
            
            elif (actividad[:4]=="FAMA" or actividad[:7]=="BONANZA" or actividad[:6]=="BODEGA" or actividad[:6]=="4711" or actividad[:4]=="9602" or actividad[:5]=="VIDEO" or actividad[:10]=="NO USUARIO" or actividad[:5]=="OTROS" or actividad[:6]=="TABORA" or actividad[:6]=="Sin in" or actividad[:5]=="TURCO" or actividad[:4]=="5630" or actividad[:4]=="6120" or actividad[:4]=="5221" or actividad[:4]=="5611" or actividad[:4]=="4789" or actividad[:6]=="FABRIC" or actividad[:6]=="LITERA"):
                data["ACTIVIDAD"][i]="OTROS"
                
                
        
        for i in data.index:
            Flag=False
            for j in actividades:
                if data["ACTIVIDAD"][i]==j:
                    Flag=True
            if Flag==False and data["CATEGORIA"][i]==1:
                data["ACTIVIDAD"][i]="RESTAURANTE"
                
        
        for i in data.index:
            Flag=False
            for j in actividades:
                if data["ACTIVIDAD"][i]==j:
                    Flag=True
            if Flag==False and data["CATEGORIA"][i]==2:
                data["ACTIVIDAD"][i]="RESTAURANTE"
                
        for i in data.index:
            Flag=False
            for j in actividades:
                if data["ACTIVIDAD"][i]==j:
                    Flag=True
            if Flag==False and data["CATEGORIA"][i]==3:
                data["ACTIVIDAD"][i]="RESTAURANTE"
        
        
        
        for i in data.index:
            Flag=False
            for j in actividades:
                if data["ACTIVIDAD"][i]==j:
                    Flag=True
            if Flag==False and data["CATEGORIA"][i]==4:
                data["ACTIVIDAD"][i]="COMERCIO"
        
        MinMaxScaler(NV[0],data)
            
        for i in one_va:
            variable=pd.get_dummies(data[i],prefix=i)
            data=data.drop([i],axis=1)
            data=pd.concat([data,variable],axis=1)
            
        codigo_cate=data[["CODIGO","CATEGORIA"]]
        data=data.drop(["CODIGO","CATEGORIA"],axis=1)
        
        model=joblib.load("model_new_users.pkl")
        y_pred=model.predict(data)
        for i in range(len(y_pred)):
            y_pred[i]=round(y_pred[i],2)
            if y_pred[i]>100:
                y_pred[i]=100
            elif y_pred[i]<0:
                y_pred[i]=0
            y_pred
        
        data=devolucion
        data["PORCENTAJE DINERO PAGADO PERIODO DE VINCULACION"]=y_pred

       
    elif modelo_a_ejecutar=="antiguos clientes":
        try:
            data=pd.read_csv(StringIO(str(csv.file.read(), 'latin-1')), encoding='latin-1',sep=";")
        except:
            data=pd.read_csv(StringIO(str(csv.file.read(), 'latin-1')), encoding='latin-1')
            
        devolucion=data.copy()
        
        NV=["TARIFA BASE","TOTAL MORA PERIODO DE VINCULACION", "TOTAL DINERO PAGADO EN PERIODO DE VINCULACION", "MORA PERIODO 2009", "MORA PERIODO 2010","MORA PERIODO 2011","MORA PERIODO 2012","MORA PERIODO 2013","MORA PERIODO 2014","MORA PERIODO 2015","MORA PERIODO 2016","MORA PERIODO 2017","MORA PERIODO 2018","MORA PERIODO 2019","MORA PERIODO 2020","PAGO 2009","PAGO 2010","PAGO 2011","PAGO 2012","PAGO 2013","PAGO 2014","PAGO 2015","PAGO 2016","PAGO 2017","PAGO 2018","PAGO 2019","PAGO 2020"]
            
        variables_en_cuenta=['CODIGO','PERIODO LIQUIDADO','LOCALIDAD', 'ESTRATO', 'CATEGORIA', 'ACTIVIDAD', 'GESTION COBRO',
        'ESTADO JURIDICO', 'CONVENIO', 'TARIFA BASE', 'ACTA',
        'MORA PERIODO 2009', 'MORA PERIODO 2010', 'MORA PERIODO 2011',
        'MORA PERIODO 2012', 'MORA PERIODO 2013', 'MORA PERIODO 2014',
        'MORA PERIODO 2015', 'MORA PERIODO 2016', 'MORA PERIODO 2017',
        'MORA PERIODO 2018', 'MORA PERIODO 2019', 'MORA PERIODO 2020',
        'VALOR LIQUIDADO', 'PAGO 2009', 'PAGO 2010', 'PAGO 2011', 'PAGO 2012',
        'PAGO 2013', 'PAGO 2014', 'PAGO 2015', 'PAGO 2016', 'PAGO 2017',
        'PAGO 2018', 'PAGO 2019', 'PAGO 2020']
        
        data=data[variables_en_cuenta]
        
        for i in moras:
            data[i]=data[i].fillna(0)
            
        for j in moras:
            data[j]=data[j].astype(str)
            for i in data.index:
                string=data[j][i]
                string = ''.join( x for x in string if x not in "$.,")
                data[j][i]=string
            data[j] = data[j].astype(int, errors = 'raise')
            
        for i in moras:
            devolucion[i]=data[i]
            
        NaN={}
        for i in pagos:
            NaN[i]=data[i].isnull()
            
        for i in pagos:
            data[i]=data[i].fillna(0)
            
        for j in pagos:
            data[j]=data[j].astype(str)
            for i in data.index:
                string=data[j][i]
                string = ''.join( x for x in string if x not in "$.,")
                data[j][i]=string
            data[j] = data[j].astype(int, errors = 'raise')
            
        data["TARIFA BASE"]=data["TARIFA BASE"].astype(str)    
        for i in data.index:
            string=data["TARIFA BASE"][i]
            string = ''.join( x for x in string if x not in "$.,")
            data["TARIFA BASE"][i]=string
        data["TARIFA BASE"] = data["TARIFA BASE"].astype(int, errors = 'raise') 
        
        data["VALOR LIQUIDADO"]=data["VALOR LIQUIDADO"].astype(str)    
        for i in data.index:
            string=data["VALOR LIQUIDADO"][i]
            string = ''.join( x for x in string if x not in "$.,")
            data["VALOR LIQUIDADO"][i]=string
        data["VALOR LIQUIDADO"] = data["VALOR LIQUIDADO"].astype(int, errors = 'raise') 
            
        data["CODIGO"]=data["CODIGO"].astype(str)  
        A_vinculacion=[]
        for i in data.index:
            vinculacion=data["CODIGO"][i][:4]
            A_vinculacion.append(vinculacion)
        data["CODIGO"]=data["CODIGO"].astype(int)
        data["ANO VINCULACION"]=A_vinculacion
        data["ANO VINCULACION"]=data["ANO VINCULACION"].astype(int)
        
        devolucion["ANO VINCULACION"]=data["ANO VINCULACION"]
        
        
        A_desvinculazion=[]
        for i in data.index:
            if len(data["PERIODO LIQUIDADO"][i])==14:
                A_desvinculazion.append(data["PERIODO LIQUIDADO"][i][7:11])
            elif len(data["PERIODO LIQUIDADO"][i])==15:
                A_desvinculazion.append(data["PERIODO LIQUIDADO"][i][8:12])
            else:
                A_desvinculazion.append(data["PERIODO LIQUIDADO"][i][7:11])

        data["ANO DESVINCULACION"]=A_desvinculazion
        data["ANO DESVINCULACION"]=data["ANO DESVINCULACION"].astype(int, errors="raise")
        
        devolucion["ANO DESVINCULACION"]=data["ANO DESVINCULACION"]
        
        
        FUPC=[]
        for i in data.index:
            if data["ANO VINCULACION"][i]<=2009:
                count=0
            else:
                for j in range(1,len(años)):
                    if data["ANO VINCULACION"][i]==años[j]:
                        count=j
            FUP=False
            for k in range(len(moras)-1,count-1,-1):
                if data[moras[k]][i]==0 and FUP==False:
                    date=años[k]
                    FUP=True
            if FUP==False:
                date="0"    #Put zero if never pay full
            FUPC.append(date)
        data["ANO ULTIMO PAGO COMPLETO"]=FUPC
        data["ANO ULTIMO PAGO COMPLETO"]=data["ANO ULTIMO PAGO COMPLETO"].astype(int)
        
        devolucion["ANO ULTIMO PAGO COMPLETO"]=data["ANO ULTIMO PAGO COMPLETO"]
        
        TMPV=[]
        for j in data.index:
            sum=0
            for i in moras:
                sum=data[i][j]+sum
            TMPV.append(sum)
        data["TOTAL MORA PERIODO DE VINCULACION"]=TMPV
        
        devolucion["TOTAL MORA PERIODO DE VINCULACION"]=data["TOTAL MORA PERIODO DE VINCULACION"]
        
        NAV=[]
        for i in data.index:
            if data["ANO DESVINCULACION"][i]==data["ANO VINCULACION"][i]:
                nav=1
            else:
                nav=data["ANO DESVINCULACION"][i]-data["ANO VINCULACION"][i]
            NAV.append(nav)
        data["NUMERO DE ANOS VINCULADO"]=NAV    
        
        devolucion["NUMERO DE ANOS VINCULADO"]=data["NUMERO DE ANOS VINCULADO"]
        
        
        for i in data.index:
            if data["ANO VINCULACION"][i]<=2009:
                count=0
            else:
                for j in range(1,len(años)):
                    if data["ANO VINCULACION"][i]==años[j]:
                        count=j
            if count==0:
                for k in range(len(pagos)):
                    if NaN[pagos[k]][i]==True:
                        data[pagos[k]][i]=data["TARIFA BASE"][i]-data[moras[k]][i]
                        if data[pagos[k]][i]<0:
                            data[pagos[k]][i]=0
            else:
                for k in range(count):
                    if NaN[pagos[k]][i]==True:
                        data[pagos[k]][i]=0
                for k in range(count,len(pagos)):
                    if NaN[pagos[k]][i]==True:
                        data[pagos[k]][i]=data["TARIFA BASE"][i]-data[moras[k]][i]
                        if data[pagos[k]][i]<0:
                            data[pagos[k]][i]=0
                            
        for i in pagos:
            devolucion[i]=data[i]
                            
        
        TP=[]
        for i in data.index:
            tp=0
            for j in pagos:
                tp=tp+data[j][i]
            TP.append(tp)
        data["TOTAL DINERO PAGADO EN PERIODO DE VINCULACION"]=TP
        
        devolucion["TOTAL DINERO PAGADO EN PERIODO DE VINCULACION"]=data["TOTAL DINERO PAGADO EN PERIODO DE VINCULACION"]
        
        
        code_pl=data[["CODIGO","PERIODO LIQUIDADO"]]
        data=data.drop(["PERIODO LIQUIDADO"],axis=1)
        data=data.drop(["CODIGO"],axis=1)
        
        for i in data.index:
            actividad=data["ACTIVIDAD"][i]
            if (actividad[:5]=="HOTEL" or actividad[:9]=="BALNEARIO" or actividad[:9]=="HOSPEDAJE" or actividad[:8]=="ESTADERO" or actividad[:5]=="MOTEL" or actividad[:11]=="RESIDENCIAS" or actividad[:6]=="HOSTAL"):
                data["ACTIVIDAD"][i]="ALOJAMIENTO"

            elif (actividad[:9]=="CENTRO DE" or actividad[:3]=="CLU"):
                data["ACTIVIDAD"][i]="CLUB"

            elif (actividad[:7]=="ALMACEN" or actividad[:6]=="INTERN" or actividad[len(actividad)-8:len(actividad)]=="INTERNET" or actividad[:5]=="CABIN" or actividad[:5]=="VENTA" or actividad[:6]=="SASTRE" or actividad[:6]=="REMONT" or actividad[:8]=="SERVICIO" or actividad[:4]=="CATE"  or actividad[len(actividad)-8:len(actividad)]=="GASOLINA" or  actividad=="SALOPN DE RECEPCIONES" or actividad[:5]=="GASOL" or actividad[:3]=="SPA" or actividad[:6]=="PORTAL" or actividad[:4]=="LAVA" or actividad[:3]=="AGE" or actividad[:5]=="SAUNA" or actividad[:6]=="MANTEN" or actividad[:3]=="WEB" or actividad[:5]=="DISEN" or  actividad[:4]=="AUTO" or actividad[:8]=="COMUNICA" or actividad[:6]=="ACTIVI" or actividad[:5]=="APUES" or actividad[:6]=="PUBLIC" or actividad[:4]=="DROG" or actividad[:4]=="Drog" or actividad[:5]=="FARMA" or actividad[:3]=="ALM" or actividad[:3]=="VNT" or actividad[:7]=="NATURIS" or actividad[:5]=="ARTIC" or actividad[:6]=="CALZAD" or actividad[:7]=="GIGARRE" or actividad[:4]=="ALAM" or actividad[:7]=="CERAMIC" or actividad[:6]=="Tienda" or actividad[:7]=="COGARRE" or actividad[:5]=="ALMAC" or actividad[:5]=="Almac" or actividad[:6]=="TIENDA" or actividad[:3]=="CIG" or actividad[:5]=="SIGAR" or actividad[:7]=="cigarre" or actividad[len(actividad)-10:len(actividad)]=="CIGARRERIA" or actividad[:12]=="AUTOSERVICIO" or actividad[:3]=="VTA" or actividad[:8]=="LICORERA" or actividad[:3]=="FRU" or actividad[:9]=="PAPELERIA" or actividad[:7]=="CONSECI" or actividad[:13]=="CONCESIONARIO" or actividad[:4]=="JOYE" or actividad[:6]=="FERRET" or actividad[:7]=="CARNICE" or actividad[len(actividad)-10:len(actividad)]=="CARNICERIA" or actividad[:7]=="CACHARR" or actividad[:6]=="BICICL" or actividad[:5]=="VIDRI" or actividad[:6]=="MISCEL" or actividad[:5]=="OPTIC" or actividad[:5]=="DULCE" or actividad[:4]=="CASA" or actividad[:7]=="MINIMAR" or actividad[:7]=="MINIMER" or actividad[:6]=="CENTRO" or actividad[:13]=="DISTRIBUIDORA" or actividad[:6]=="Distri" or actividad[:7]=="ESTUDIO" or actividad[:11]=="GRIFORIFICO" or actividad[:7]=="GRANDES" or actividad[:6]=="KIOSCO" or actividad[:8]=="IMPRENTA" or actividad[:7]=="OFICINA" or actividad[:6]=="TIENDA" or actividad[:5]=="SUPER" or actividad[len(actividad)-7:len(actividad)]=="MERCADO" or actividad[:6]=="COMERC" or actividad[:11]=="CORPORACION"):
                data["ACTIVIDAD"][i]="COMERCIO"

            elif (actividad[:9]=="CAFETERIA" or actividad[:5]=="SALON" or actividad[:9]=="HELADERIA" or actividad[:3]=="TDA" or actividad[len(actividad)-5:len(actividad)]=="LICOR" or actividad[len(actividad)-8:len(actividad)]=="LICORERA" or actividad[:6]=="ROKOLA" or actividad[:4]=="ROCK" or actividad[len(actividad)-7:len(actividad)]=="CERVEZA" or actividad[len(actividad)-8:len(actividad)]=="BOLIRANA" or actividad[len(actividad)-7:len(actividad)]=="LICORES" or actividad[len(actividad)-8:len(actividad)]=="LICORES " or actividad[len(actividad)-3:len(actividad)]=="LUZ" or actividad[:6]=="EXPEND" or actividad[:5]=="LICOR"):
                data["ACTIVIDAD"][i]="CONSUMO DE LICOR"

            elif (actividad[:8]=="ESTADERO" or actividad[:5]=="DISCO" or actividad[:5]=="DISKO" or actividad[:5]=="Disco" or actividad[:4]=="Club" or actividad[:8]=="WISKERIA" or actividad[:3]=="BAR" or actividad[:4]==" BAR" or actividad[:3]=="bar" or actividad[len(actividad)-4:len(actividad)]==" BAR" or actividad[:7]=="TABERNA" or actividad[:5]=="FONDA"):
                data["ACTIVIDAD"][i]="DISCOTECA"

            elif (actividad[:6]=="TEATRO" or actividad[:4]=="SALA" or actividad[:4]=="jueg" or actividad[:4]=="CANH" or actividad[:4]=="ACAE" or actividad[:6]=="CASINO" or actividad[:5]=="BINGO" or actividad[:7]=="GALLERA" or actividad[:6]=="CACINO" or actividad[:10]=="CENTRO DEP" or actividad[:4]=="CINE" or actividad[:6]=="PARQUE" or actividad[:6]=="BILLAR" or actividad[len(actividad)-6:len(actividad)]=="BILLAR" or actividad[len(actividad)-6:len(actividad)]=="BILLAR" or actividad[:3]=="BOL" or actividad[:8]=="GIMNASIO" or actividad[:3]=="GYM" or actividad[:3]=="GIM" or actividad[:5]=="TRAIN" or actividad[:6]=="CANCHA" or actividad[:5]=="CAMPO" or actividad[:5]=="JUEGO" or actividad[:7]=="KARAOKE" or actividad[len(actividad)-4:len(actividad)]=="TEJO" or actividad[:4]=="RANO" or actividad[len(actividad)-6:len(actividad)==" BAILE"]):
                data["ACTIVIDAD"][i]="ENTRETENIMIENTO"

            elif (actividad[:9]=="CAFETERIA" or actividad[:11]=="RESTAURANTE" or actividad[:11]=="POLLE" or actividad[:5]=="PIZZE" or actividad[:7]=="PIQUETE" or actividad[:8]=="SALSAMEN" or actividad[:7]=="ASADERO" or actividad[:6]=="COMIDA" or actividad[:5]=="PANAD" or actividad[:4]=="CAFE" or actividad[len(actividad)-4:len(actividad)]=="CAFE") or actividad[:4]=="PAST":
                data["ACTIVIDAD"][i]="RESTAURANTE"

            elif (actividad[:10]=="CENTRO MED" or actividad[:7]=="CLINICA" or actividad[:7]=="Clinica"   or actividad[:11]=="CONSULTORIO" or actividad[:8]=="HOSPITAL" or actividad[:5]=="MEDIC" or actividad[len(actividad)-2:len(actividad)]=="PS"  or actividad[len(actividad)-6:len(actividad)]=="FISICO" or actividad[:6]=="ACONDI"):
                data["ACTIVIDAD"][i]="SALUD"

            elif (actividad[:12]=="CENTRO DE ES" or actividad[:7]=="ENTIDAD" or actividad[:5]=="SDALA" or actividad[:6]=="Taller" or actividad[:6]==" TALLER"    or actividad[:5]=="ESTET"   or actividad[:6]=="TALLER" or actividad[:4]=="PELU" or actividad[:8]=="ESTETICA" or actividad[:15]=="SALA DE BELLEZA" or actividad[len(actividad)-8:len(actividad)]=="Estetica"):
                data["ACTIVIDAD"][i]="SERVICIOS"

            elif (actividad[:8]=="TERMINAL" or actividad[:10]=="AEROPUERTO" or actividad[:14]=="TRANSPORTADORA"):
                data["ACTIVIDAD"][i]="TRANSPORTE"
            
            elif (actividad[:10]=="ENTIDAD ED" or actividad[:8]=="ACADEMIA" or actividad[len(actividad)-6:len(actividad)]=="JARDIN"):
                data["ACTIVIDAD"][i]="EDUCACION"
            
            elif (actividad[:7]=="IGLESIA"):
                data["ACTIVIDAD"][i]="RELIGIOSO"
            
            elif (actividad[:4]=="FAMA" or actividad[:7]=="BONANZA" or actividad[:6]=="BODEGA" or actividad[:6]=="4711" or actividad[:4]=="9602" or actividad[:5]=="VIDEO" or actividad[:10]=="NO USUARIO" or actividad[:5]=="OTROS" or actividad[:6]=="TABORA" or actividad[:6]=="Sin in" or actividad[:5]=="TURCO" or actividad[:4]=="5630" or actividad[:4]=="6120" or actividad[:4]=="5221" or actividad[:4]=="5611" or actividad[:4]=="4789" or actividad[:6]=="FABRIC" or actividad[:6]=="LITERA"):
                data["ACTIVIDAD"][i]="OTROS"
                
                
        
        for i in data.index:
            Flag=False
            for j in actividades:
                if data["ACTIVIDAD"][i]==j:
                    Flag=True
            if Flag==False and data["CATEGORIA"][i]==1:
                data["ACTIVIDAD"][i]="RESTAURANTE"
                
        
        for i in data.index:
            Flag=False
            for j in actividades:
                if data["ACTIVIDAD"][i]==j:
                    Flag=True
            if Flag==False and data["CATEGORIA"][i]==2:
                data["ACTIVIDAD"][i]="RESTAURANTE"
                
        for i in data.index:
            Flag=False
            for j in actividades:
                if data["ACTIVIDAD"][i]==j:
                    Flag=True
            if Flag==False and data["CATEGORIA"][i]==3:
                data["ACTIVIDAD"][i]="RESTAURANTE"
        
        
        
        for i in data.index:
            Flag=False
            for j in actividades:
                if data["ACTIVIDAD"][i]==j:
                    Flag=True
            if Flag==False and data["CATEGORIA"][i]==4:
                data["ACTIVIDAD"][i]="COMERCIO"
                
        devolucion["ACTIVIDAD"]=data["ACTIVIDAD"]
        
        categoria=data["CATEGORIA"]
        data=data.drop(["CATEGORIA"],axis=1)
        
        
        PDP=[]
        for i in data.index:
            dinero_total=full_money(pagos,moras,i,data)
            pagado_total=data["TOTAL DINERO PAGADO EN PERIODO DE VINCULACION"][i]
            porcentaje_pagado=(pagado_total*100)/dinero_total
            if porcentaje_pagado>100:
                porcentaje_pagado=100
            if porcentaje_pagado==np.inf:
                porcentaje_pagado=100
            PDP.append(porcentaje_pagado)
        data["PORCENTAJE DINERO PAGADO PERIODO DE VINCULACION"]=PDP
        for i in data.index:
            data["PORCENTAJE DINERO PAGADO PERIODO DE VINCULACION"][i]=round(data["PORCENTAJE DINERO PAGADO PERIODO DE VINCULACION"][i],2)
            
        devolucion["PORCENTAJE DINERO PAGADO PERIODO DE VINCULACION"]=data["PORCENTAJE DINERO PAGADO PERIODO DE VINCULACION"]
        
        data=devolucion
        """"
        data_model=data.copy()
        
        for i in NV:
            MinMaxScaler(i,data_model)
            
        for i in one_va:
            variable=pd.get_dummies(data_model[i],prefix=i)
            data_model=data_model.drop([i],axis=1)
            data_model=pd.concat([data_model,variable],axis=1)
            
        model=joblib.load("modelo_old_users.pkl")
        y_pred=model.predict(data_model)
        for i in range(len(y_pred)):
            if y_pred[i]>100:
                y_pred[i]=100
            elif y_pred[i]<0:
                y_pred[i]=0
                
        df=pd.DataFrame()
        df["PORCENTAJE DINERO PAGADO"]=y_pred
        df["PORCENTAJE DINERO PAGADO"]=df["PORCENTAJE DINERO PAGADO"].astype(int)"""
        
        
    if modelo_a_ejecutar=="antiguos clientes" or modelo_a_ejecutar=="nuevos clientes":
        data.to_csv("csv_procesado.csv",index=False,sep=";")
        return FileResponse("csv_procesado.csv") 
    else:
        return {"ERROR":"El modelo ingresado no existe"}
    
    