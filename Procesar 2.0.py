import pandas as pd
from pathlib import Path
import numpy as np
from datetime import datetime, timedelta
import sys
import time
from tempfile import NamedTemporaryFile
import shutil

def safe_save_excel(df, filepath, columns=None, max_retries=3, wait_time=1):
    """Guarda un DataFrame en un archivo Excel con manejo de errores y reintentos"""
    if columns is None:
        columns = df.columns
        
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    for attempt in range(max_retries):
        try:
            # Usar archivo temporal
            with NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                temp_path = tmp.name
            
            # Guardar en archivo temporal
            with pd.ExcelWriter(temp_path, engine='openpyxl') as writer:
                df[columns].to_excel(writer, index=False, sheet_name='Consolidado')
                
                # Aplicar formato si es necesario
                workbook = writer.book
                worksheet = writer.sheets['Consolidado']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = (max_length + 2)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Mover a ubicación final
            shutil.move(temp_path, filepath)
            return True
            
        except PermissionError:
            if attempt == max_retries - 1:
                raise
            print(f"Permiso denegado, reintentando en {wait_time} segundos... (intento {attempt + 1})")
            time.sleep(wait_time)
            if Path(temp_path).exists():
                Path(temp_path).unlink()
        except Exception as e:
            if Path(temp_path).exists():
                Path(temp_path).unlink()
            raise e
    return False

def obtener_rutas():
    """Configura las rutas relativas desde la ubicación del script"""
    try:
        ruta_script = Path(__file__).resolve().parent
        print(f"Ruta del script: {ruta_script}")

        rutas = {
            'AC': ruta_script / 'RIPS' / 'AC',
            'AP': ruta_script / 'RIPS' / 'AP',
            'AM': ruta_script / 'RIPS' / 'AM',
            'AT': ruta_script / 'RIPS' / 'AT',
            'AH': ruta_script / 'RIPS' / 'AH',
            'AN': ruta_script / 'RIPS' / 'AN',
            'consolidado': ruta_script / '_INFORME',
            'HOSVITAL': ruta_script / 'HOSVITAL',
            'cups': ruta_script / 'Resolucion CUPS.xlsx'
        }
        
        for nombre, ruta in rutas.items():
            if nombre not in ['cups', 'HOSVITAL']:  # No crear HOSVITAL automáticamente
                ruta.mkdir(parents=True, exist_ok=True)
                print(f"Carpeta {nombre} verificada/creada: {ruta}")
        
        # Verificar si existe HOSVITAL pero no crearla
        if rutas['HOSVITAL'].exists():
            print(f"Carpeta HOSVITAL encontrada: {rutas['HOSVITAL']}")
        else:
            print(f"Advertencia: Carpeta HOSVITAL no encontrada en {rutas['HOSVITAL']}")
        
        return rutas
    except Exception as e:
        print(f"Error al configurar las rutas: {str(e)}")
        sys.exit(1)

def cargar_cups():
    """Cargar archivo CUPS desde la misma carpeta del script"""
    try:
        ruta_script = Path(__file__).resolve().parent
        ruta_cups = ruta_script / 'Resolucion CUPS.xlsx'
        print("\nCargando archivo CUPS...")
        if not ruta_cups.exists():
            print("Advertencia: Archivo CUPS no encontrado. Se continuará sin mapeo de códigos.")
            return {}
        cups_df = pd.read_excel(ruta_cups, usecols=['CUPS', 'DESCRIPCION CUPS'])
        return dict(zip(cups_df['CUPS'], cups_df['DESCRIPCION CUPS']))
    except Exception as e:
        print(f"Error al cargar archivo CUPS: {str(e)}")
        return {}

def obtener_encabezados(tipo_archivo):
    """Retorna los encabezados según el tipo de archivo RIPS"""
    encabezados = {
        'AC': [
            "Factura", "Cod_IPS", "Tipo_id", "Identificacion", "Fecha",
            "Autorizacion", "cod_servicio", "finalidad", "causa_externa",
            "dx_principal", "dx_relacionado1", "dx_relacionado2",
            "dx_relacionado3", "tipo_dx", "Valor", "valor_moderadora", "valor_neto"
        ],
        'AP': [
            "Factura", "Cod_IPS", "Tipo_id", "Identificacion", "Fecha", "Autorizacion",
            "cod_servicio", "ambito", "finalidad", "personal_atiende", "dx_principal",
            "dx_relacionado", "dx_complicacion", "forma_realizacion", "Valor"
        ],
        'AM': [
            "Factura", "Cod_IPS", "Tipo_id", "Identificacion", "Autorizacion",
            "cod_servicio", "tipo_medicamento", "Nombre_servicio", "forma_farmaceutica",
            "concentracion", "unidad_medida", "Cantidad", "Valor", "valor_total"
        ],
        'AT': [
            "Factura", "Cod_IPS", "Tipo_id", "Identificacion", "Autorizacion", "tipo_servicio",
            "cod_servicio", "Nombre_servicio", "Cantidad", "Valor", "valor_total"
        ],
        'AH': [
            "Factura", "Cod_IPS", "Tipo_id", "Identificacion", "Cod", "Fecha_ingreso", "Hora_ingreso", "Cod2",
            "Autorizacion", "dx_principal", "dx_relacionado1", "dx_relacionado2", "dx_relacionado3", "dx_relacionado4",
            "dx_relacionado5", "Cod3", "Cod4", "Fecha_salida", "Hora_salida"
        ],
        'AN': [
            "Factura", "Cod_IPS", "Tipo_id", "Identificacion", "Autorizacion", "cod_servicio",
            "tipo_anexo", "Nombre_servicio", "Cantidad", "Valor", "valor_total"
        ]
    }
    return encabezados.get(tipo_archivo, [])

def actualizar_codigos_servicio(df):
    """Actualiza los códigos de servicio inválidos basándose en el nombre del servicio"""
    valores_invalidos = ['', 'null', 'NA', 'N/A', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    
    df['cod_servicio'] = df['cod_servicio'].astype(str)
    df['cod_servicio'] = df['cod_servicio'].apply(convertir_fecha_a_numero)
    
    mascara_invalidos = (
        df['cod_servicio'].isna() | 
        df['cod_servicio'].isin(valores_invalidos) | 
        df['cod_servicio'].astype(str).str.contains(r'\d{2}/\d{2}/\d{4}')
    )
    
    mapeo_codigos = df[~mascara_invalidos].groupby('Nombre_servicio')['cod_servicio'].first().to_dict()
    df.loc[mascara_invalidos, 'cod_servicio'] = df.loc[mascara_invalidos, 'Nombre_servicio'].map(mapeo_codigos).fillna('servicio_no_identificado')

    if 'Cantidad' not in df.columns:
        df['Cantidad'] = 1

    return df

def procesar_archivo(ruta_archivo, cups_dict=None, tipo_archivo=''):
    """Procesa cada archivo RIPS con verificación de columnas extra"""
    encabezados = obtener_encabezados(tipo_archivo)
    
    try:
        with open(ruta_archivo, 'r', encoding='latin1') as file:
            lineas = file.readlines()
        
        lineas_procesadas = []
        i = 0
        while i < len(lineas):
            linea_actual = lineas[i].replace('"', '').strip()
            columnas_actual = len(linea_actual.split(',')) if ',' in linea_actual else len(linea_actual.split(';'))
            
            if columnas_actual < len(encabezados):
                if i + 1 < len(lineas):
                    linea_siguiente = lineas[i+1].replace('"', '').strip()
                    linea_actual += linea_siguiente
                    i += 1
            
            lineas_procesadas.append(linea_actual)
            i += 1
        
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='latin1') as temp_file:
            temp_file.write('\n'.join(lineas_procesadas))
            temp_file_path = temp_file.name
        
        try:
            df_raw = pd.read_csv(temp_file_path, encoding='latin1', header=None, sep=',', quoting=pd.io.common.QUOTE_NONE, on_bad_lines='skip')
        except:
            try:
                df_raw = pd.read_csv(temp_file_path, encoding='latin1', header=None, sep=';', quoting=pd.io.common.QUOTE_NONE, on_bad_lines='skip')
            except:
                df_raw = pd.read_csv(temp_file_path, encoding='latin1', header=None, sep=',|;', engine='python', on_bad_lines='skip')
        
        import os
        os.unlink(temp_file_path)
    
    except Exception as e:
        print(f"Error procesando el archivo {ruta_archivo}: {str(e)}")
        return pd.DataFrame()

    # Procesamiento específico para archivo AH
    if tipo_archivo == 'AH':
        # Asignar las columnas directamente según la estructura real del archivo AH
        if len(df_raw.columns) >= 19:  # Asegurarnos que tenemos al menos 19 columnas
            df = pd.DataFrame()
            df['Factura'] = df_raw[0]
            df['Cod_IPS'] = df_raw[1]
            df['Tipo_id'] = df_raw[2]
            df['Identificacion'] = df_raw[3]
            df['Cod'] = df_raw[4]  # Columna adicional que no estaba en los encabezados originales
            df['Fecha_ingreso'] = df_raw[5]
            df['Hora_ingreso'] = df_raw[6]
            df['Cod2'] = df_raw[7]  # Otra columna adicional
            df['Autorizacion'] = df_raw[8]
            df['dx_principal'] = df_raw[9]
            df['dx_relacionado1'] = df_raw[10]
            df['dx_relacionado2'] = df_raw[11]
            df['dx_relacionado3'] = df_raw[12]
            df['dx_relacionado4'] = df_raw[13]
            df['dx_relacionado5'] = df_raw[14]
            df['Cod3'] = df_raw[15]  # Otra columna adicional
            df['Cod4'] = df_raw[16]  # Otra columna adicional
            df['Fecha_salida'] = df_raw[17]
            df['Hora_salida'] = df_raw[18]
        else:
            # Si no tiene suficientes columnas, crear un DataFrame con las columnas esenciales
            df = pd.DataFrame(columns=['Factura', 'Cod_IPS', 'Tipo_id', 'Identificacion', 
                                     'Fecha_ingreso', 'Fecha_salida', 'dx_principal',
                                     'dx_relacionado1', 'dx_relacionado2', 'Autorizacion'])
            # Intentar asignar las columnas que sí existen
            for i in range(min(len(df_raw.columns), 19)):
                if i == 0: df['Factura'] = df_raw[i]
                elif i == 1: df['Cod_IPS'] = df_raw[i]
                elif i == 2: df['Tipo_id'] = df_raw[i]
                elif i == 3: df['Identificacion'] = df_raw[i]
                elif i == 5: df['Fecha_ingreso'] = df_raw[i]
                elif i == 9: df['dx_principal'] = df_raw[i]
                elif i == 10: df['dx_relacionado1'] = df_raw[i]
                elif i == 11: df['dx_relacionado2'] = df_raw[i]
                elif i == 8: df['Autorizacion'] = df_raw[i]
                elif i == 17: df['Fecha_salida'] = df_raw[i]
        
        # Para AH no procesamos cod_servicio ni Nombre_servicio
        df['CIE10'] = df['dx_principal'] if 'dx_principal' in df.columns else pd.NA
        df['Cantidad'] = 1  # Hospitalizaciones se cuentan como 1
        df['Nombre_servicio'] = 'Hospitalización'  # Valor por defecto para AH
        df['cod_servicio'] = pd.NA  # No existe en AH
        
    else:
        # Procesamiento normal para otros archivos (AC, AP, AM, AT, AN)
        if len(df_raw.columns) > len(encabezados):
            df_raw = df_raw.iloc[:, :len(encabezados)]
        elif len(df_raw.columns) < len(encabezados):
            for i in range(len(df_raw.columns), len(encabezados)):
                df_raw[i] = pd.NA

        df = df_raw.copy()
        df.columns = encabezados[:len(df.columns)]

        # Procesar cod_servicio solo para archivos que lo tienen
        if 'cod_servicio' in df.columns:
            df['cod_servicio'] = df['cod_servicio'].apply(convertir_fecha_a_numero)
            
            if tipo_archivo in ['AC', 'AP'] and cups_dict:
                df['cod_servicio'] = df['cod_servicio'].astype(str).str.strip()
                df['cod_servicio'] = df['cod_servicio'].replace(["$", " ", ".", ""], "")
                df['Nombre_servicio'] = df['cod_servicio'].map(cups_dict)
                df['Cantidad'] = 1

        # Para AM, AT, AN que tienen Nombre_servicio
        if tipo_archivo in ['AM', 'AT', 'AN'] and 'cod_servicio' in df.columns:
            df = actualizar_codigos_servicio(df)

        # Extraer CIE10 según el tipo de archivo
        if tipo_archivo in ['AC', 'AP']:
            df['CIE10'] = df['dx_principal'] if 'dx_principal' in df.columns else pd.NA
        else:
            df['CIE10'] = pd.NA

    # Procesamiento común para todos los tipos
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace('"', '').str.strip()

    nombre_archivo = Path(ruta_archivo).name
    df['Archivo'] = nombre_archivo[:2]
    df['Periodo'] = nombre_archivo[2:-4]

    df['Identificacion'] = df['Identificacion'].astype(str).str.replace(r'\.0$', '', regex=True)
    df['Identificacion'] = df['Identificacion'].str.replace(r'[.,]', '', regex=True)
    
    # Crear clave única
    if tipo_archivo == 'AH':
        df['Key'] = (df['Factura'].astype(str) + '-' + 
                    df['Cod_IPS'].astype(str) + '-' + 
                    df['Identificacion'].astype(str) + '-' + 
                    df['Periodo'].astype(str) + '-' + 
                    df['Autorizacion'].astype(str))
    else:
        df['Key'] = (df['Factura'].astype(str) + '-' + 
                    df['Cod_IPS'].astype(str) + '-' + 
                    df['Identificacion'].astype(str) + '-' + 
                    df['Periodo'].astype(str) + '-' + 
                    (df['Nombre_servicio'] if 'Nombre_servicio' in df.columns else df.get('cod_servicio', '')).astype(str))

    # Asegurar columnas requeridas
    columnas_requeridas = ['Factura', 'Nombre_servicio', 'Cantidad', 'CIE10']
    for col in columnas_requeridas:
        if col not in df.columns:
            df[col] = np.nan

    return df

def convertir_fecha_a_numero(valor):
    import re
    from datetime import datetime
    
    valor = str(valor).strip()
    patron_fecha = r'^\d{2}/\d{2}/\d{4}$'
    
    if re.match(patron_fecha, valor):
        try:
            fecha = datetime.strptime(valor, '%d/%m/%Y')
            base = datetime(1900, 1, 1)
            dias = (fecha - base).days + 1
            if dias > 59:
                dias += 1
            return dias
        except:
            return valor
    
    return valor

def formatear_fecha(fecha):
    if pd.isna(fecha) or fecha is None or str(fecha).strip() == '':
        return None
    try:
        fecha_dt = datetime.strptime(str(fecha), '%d/%m/%Y')
        return f"{fecha_dt.year}-{fecha_dt.month:02d}"
    except:
        return None

def calcular_dias_internacion(df_ah, df_ac_ap):
    """Calcula días de internación basado en coincidencia de identificación y fechas"""
    if df_ah.empty or df_ac_ap.empty:
        return df_ac_ap  # Devuelve el DataFrame original si no hay datos para procesar
    
    # Hacer copias para no modificar los DataFrames originales
    df_ah_procesado = df_ah.copy()
    df_ac_ap_procesado = df_ac_ap.copy()
    
    # Convertir fechas en AH
    df_ah_procesado['Fecha_ingreso_dt'] = pd.to_datetime(
        df_ah_procesado['Fecha_ingreso'], format='%d/%m/%Y', errors='coerce'
    )
    df_ah_procesado['Fecha_salida_dt'] = pd.to_datetime(
        df_ah_procesado['Fecha_salida'], format='%d/%m/%Y', errors='coerce'
    )
    
    # Calcular días de internación
    df_ah_procesado['Dias_Internacion'] = (
        (df_ah_procesado['Fecha_salida_dt'] - df_ah_procesado['Fecha_ingreso_dt']).dt.days + 1
    )
    
    # Asegurar que no haya valores negativos o nulos
    df_ah_procesado['Dias_Internacion'] = df_ah_procesado['Dias_Internacion'].apply(
        lambda x: max(1, x) if pd.notnull(x) and x > 0 else 0
    )
    
    # Convertir fechas en AC/AP
    df_ac_ap_procesado['Fecha_dt'] = pd.to_datetime(
        df_ac_ap_procesado['Fecha'], format='%d/%m/%Y', errors='coerce'
    )
    
    def asignar_dias_internacion(row):
        if pd.isnull(row['Fecha_dt']):
            return 0
            
        # Buscar hospitalizaciones para este paciente
        mascara = (
            (df_ah_procesado['Identificacion'] == row['Identificacion']) &
            (row['Fecha_dt'] >= df_ah_procesado['Fecha_ingreso_dt']) &
            (row['Fecha_dt'] <= df_ah_procesado['Fecha_salida_dt'])
        )
        
        hospitalizaciones = df_ah_procesado[mascara]
        
        # Verificar coincidencia en diagnóstico si existe CIE10
        if 'CIE10' in row and pd.notnull(row['CIE10']):
            for _, hosp in hospitalizaciones.iterrows():
                dx_coincide = (
                    (str(row['CIE10']) == str(hosp.get('dx_principal', ''))) or
                    (str(row['CIE10']) == str(hosp.get('dx_relacionado1', ''))) or
                    (str(row['CIE10']) == str(hosp.get('dx_relacionado2', '')))
                )
                if dx_coincide:
                    return hosp['Dias_Internacion']
        
        # Si no hay coincidencia de diagnóstico pero sí de fechas, devolver los días
        if not hospitalizaciones.empty:
            return hospitalizaciones.iloc[0]['Dias_Internacion']
        
        return 0
    
    # Aplicar la función a cada fila del DataFrame AC/AP
    df_ac_ap_procesado['Dias_Internacion'] = df_ac_ap_procesado.apply(asignar_dias_internacion, axis=1)
    
    return df_ac_ap_procesado

def procesar_hosvital(rutas):
    """Procesa archivos HOSVITAL o devuelve DataFrame vacío si no existe la carpeta"""
    try:
        if not rutas['HOSVITAL'].exists():
            print("\nAdvertencia: No se encontró la carpeta HOSVITAL. Se usará 'NO HAY POBLACION IDENTIFICADA'")
            return pd.DataFrame(columns=['Key-Ips', 'Municipio', 'Departamento', 'Periodo'])
        
        print("\nCargando archivos Hosvital...")
        hosvital_files = list(rutas['HOSVITAL'].glob('*.xlsx'))
        
        if not hosvital_files:
            print("Advertencia: No se encontraron archivos .xlsx en HOSVITAL")
            return pd.DataFrame(columns=['Key-Ips', 'Municipio', 'Departamento', 'Periodo'])
        
        hosvital_dfs = []
        
        for archivo in hosvital_files:
            try:
                df_hosvital = pd.read_excel(archivo)
                
                # Buscar columnas relevantes con manejo de errores
                col_identificacion = next(
                    (col for col in df_hosvital.columns 
                     if any(x in col.lower() for x in ['número de documento', 'numero de documento', 'identificación'])),
                    None
                )
                
                if col_identificacion is None:
                    print(f"Advertencia: No se encontró columna de identificación en {archivo.name}")
                    continue
                
                col_municipio = next(
                    (col for col in df_hosvital.columns if 'municipio' in col.lower() and 'afili' in col.lower()),
                    None
                )
                
                col_departamento = next(
                    (col for col in df_hosvital.columns if 'departamento' in col.lower()),
                    None
                )
                
                df_hosvital['Key-Ips'] = archivo.stem[:7] + '-' + df_hosvital[col_identificacion].astype(str)
                
                # Seleccionar columnas basado en lo que existe
                columns_to_select = ['Key-Ips']
                if col_municipio:
                    columns_to_select.append(col_municipio)
                if col_departamento:
                    columns_to_select.append(col_departamento)
                
                hosvital_dfs.append(df_hosvital[columns_to_select])
                
            except Exception as e:
                print(f"Error procesando archivo {archivo.name}: {str(e)}")
                continue
        
        if not hosvital_dfs:
            print("Advertencia: No se pudo procesar ningún archivo HOSVITAL")
            return pd.DataFrame(columns=['Key-Ips', 'Municipio', 'Departamento', 'Periodo'])
        
        hosvital_consolidado = pd.concat(hosvital_dfs, ignore_index=True)
        hosvital_consolidado['Periodo'] = hosvital_consolidado['Key-Ips'].str.split('-').str[0]
        
        # Renombrar columnas para consistencia
        if col_municipio:
            hosvital_consolidado = hosvital_consolidado.rename(columns={col_municipio: 'Municipio'})
        else:
            hosvital_consolidado['Municipio'] = 'NO HAY POBLACION IDENTIFICADA'
        
        if col_departamento:
            hosvital_consolidado = hosvital_consolidado.rename(columns={col_departamento: 'Departamento'})
        else:
            hosvital_consolidado['Departamento'] = 'NO HAY POBLACION IDENTIFICADA'
        
        # Agrupar por las columnas disponibles
        group_cols = ['Periodo', 'Municipio']
        if 'Departamento' in hosvital_consolidado.columns:
            group_cols.insert(1, 'Departamento')
        
        hosvital_consolidado_final = hosvital_consolidado.groupby(group_cols).size().reset_index(name='Cantidad')
        
        return hosvital_consolidado_final
    
    except Exception as e:
        print(f"Error inesperado al procesar HOSVITAL: {str(e)}")
        return pd.DataFrame(columns=['Key-Ips', 'Municipio', 'Departamento', 'Periodo'])

def procesar_rips():
    """Proceso principal modificado para generar XLSX con formato adecuado"""
    try:
        rutas = obtener_rutas()
        cups_dict = cargar_cups()

        # Procesar HOSVITAL con manejo de errores
        hosvital_consolidado_final = procesar_hosvital(rutas)
        
        # Crear DataFrame vacío para el merge si no hay datos HOSVITAL
        if hosvital_consolidado_final.empty:
            hosvital_for_merge = pd.DataFrame(columns=['Key-Ips', 'Municipio', 'Departamento'])
        else:
            hosvital_for_merge = hosvital_consolidado_final[['Key-Ips', 'Municipio', 'Departamento']].drop_duplicates('Key-Ips')

        # Procesar archivos RIPS
        tipos_procesar = ['AC', 'AP', 'AM', 'AT', 'AH', 'AN']
        dfs = {}
        
        print("\nProcesando archivos RIPS...")
        for tipo in tipos_procesar:
            try:
                archivos = list(rutas[tipo].glob('*.txt'))
                if not archivos:
                    print(f"Advertencia: No se encontraron archivos {tipo}")
                    continue
                
                print(f"\nProcesando {len(archivos)} archivos {tipo}...")
                total_registros = 0
                
                for archivo in archivos:
                    try:
                        df_temp = procesar_archivo(archivo, cups_dict, tipo)
                        registros = len(df_temp)
                        total_registros += registros
                        print(f"✓ {archivo.name} - {registros:,} registros")
                        
                        if tipo in dfs:
                            dfs[tipo] = pd.concat([dfs[tipo], df_temp])
                        else:
                            dfs[tipo] = df_temp
                    except Exception as e:
                        print(f"Error procesando archivo {archivo.name}: {str(e)}")
                        continue
                
                print(f"Total {tipo}: {total_registros:,} registros")
            except Exception as e:
                print(f"Error procesando tipo {tipo}: {str(e)}")
                continue

        # Aplicar cálculo de días de internación si hay datos de AH y AC/AP
        if 'AH' in dfs and ('AC' in dfs or 'AP' in dfs):
            print("\nCalculando días de internación...")
            if 'AC' in dfs:
                dfs['AC'] = calcular_dias_internacion(dfs['AH'], dfs['AC'])
            if 'AP' in dfs:
                dfs['AP'] = calcular_dias_internacion(dfs['AH'], dfs['AP'])
        
        # Eliminar AH del diccionario para que no se incluya en el consolidado
        if 'AH' in dfs:
            del dfs['AH']
        
        # Procesar archivos con fechas
        tipos_con_fecha = ['AM', 'AT', 'AN']
        for tipo in tipos_con_fecha:
            if tipo in dfs:
                dfs[tipo]['Fecha'] = dfs[tipo]['Key'].map(
                    pd.concat([dfs.get('AC', pd.DataFrame()), 
                              dfs.get('AP', pd.DataFrame())])
                    .drop_duplicates('Key')
                    .set_index('Key')['Fecha']
                )

        print("\nConsolidando archivos...")
        columnas_requeridas = ['Archivo', 'Periodo', 'Cod_IPS', 'Identificacion', 'Fecha', 
                              'Factura', 'cod_servicio', 'Nombre_servicio', 'Valor', 
                              'Cantidad', 'CIE10', 'Dias_Internacion']
        
        dfs_para_consolidado = []
        for tipo, df in dfs.items():
            if tipo != 'AH':  # Excluir explícitamente AH
                for col in columnas_requeridas:
                    if col not in df.columns:
                        df[col] = np.nan
                dfs_para_consolidado.append(df[columnas_requeridas])
        
        if not dfs_para_consolidado:
            print("Advertencia: No hay datos para consolidar. Verifique que existan archivos RIPS.")
            return
        
        consolidado = pd.concat(dfs_para_consolidado, ignore_index=True)
        
        if 'Factura' not in consolidado.columns:
            consolidado['Factura'] = ''
        
        # Creación de claves
        consolidado['Key'] = (
            consolidado['Factura'].astype(str) + '-' +
            consolidado['Cod_IPS'].astype(str) + '-' +
            consolidado['Identificacion'].astype(str) + '-' +
            consolidado['Periodo'].astype(str) + '-' +
            consolidado.apply(
                lambda row: str(row['Nombre_servicio']) 
                if pd.isna(row.get('cod_servicio')) or 
                       str(row.get('cod_servicio', '')) in ['', 'null', 'NA', 'N/A', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'] 
                else str(row.get('cod_servicio', '')),
                axis=1
            )
        )
        
        consolidado['Key2'] = (
            consolidado['Cod_IPS'].astype(str) + '-' +
            consolidado['Identificacion'].astype(str) + '-' +
            consolidado['Fecha'].astype(str) + '-' +
            consolidado.apply(
                lambda row: str(row['Nombre_servicio']) 
                if pd.isna(row.get('cod_servicio')) or 
                       str(row.get('cod_servicio', '')) in ['', 'null', 'NA', 'N/A', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'] 
                else str(row.get('cod_servicio', '')),
                axis=1
            )
        )
        
        consolidado['Valor'] = pd.to_numeric(consolidado['Valor'], errors='coerce')
        consolidado['Cantidad'] = pd.to_numeric(consolidado['Cantidad'], errors='coerce')

        print("\nProcesando consolidado...")
        consolidado_final = consolidado.groupby('Key').agg({
            'Key2':'first',
            'Cod_IPS':'first',
            'Archivo': 'first',
            'Periodo': 'first',
            'Identificacion': 'first',
            'Fecha': 'first',
            'Factura': 'first',
            'cod_servicio': 'first',
            'Valor': 'mean',
            'Cantidad': 'sum',
            'Nombre_servicio': 'first',
            'CIE10': 'first',
            'Dias_Internacion': 'first'
        }).reset_index()
        
        consolidado_final['Valor'] = consolidado_final['Valor'].apply(lambda x: str(x).replace(".",","))
        
        consolidado_final['Key-Ips'] = np.where(
            consolidado_final['Fecha'].isna(), 
            consolidado_final['Periodo'].str[:7] + '-' + consolidado_final['Identificacion'].astype(str),
            consolidado_final['Fecha'].apply(formatear_fecha) + '-' + consolidado_final['Identificacion'].astype(str))
        
        # Hacer merge solo si hay datos HOSVITAL
        if not hosvital_for_merge.empty:
            consolidado_final = consolidado_final.merge(
                hosvital_for_merge, 
                left_on='Key-Ips', 
                right_on='Key-Ips', 
                how='left'
            )
        else:
            consolidado_final['Municipio'] = 'NO HAY POBLACION IDENTIFICADA'
            consolidado_final['Departamento'] = 'NO HAY POBLACION IDENTIFICADA'
        
        # Guardar archivos en formato XLSX con formato usando safe_save_excel
        print("\nGuardando archivos consolidados...")
        
        ruta_salida_rips = rutas['consolidado'] / 'consolidado_rips.xlsx'
        column_order_rips = [
            'Key', 'Key2', 'Key-Ips', 'Archivo', 'Periodo', 'Cod_IPS',
            'Identificacion', 'Fecha', 'Factura', 'cod_servicio', 'Nombre_servicio',
            'Valor', 'Cantidad', 'CIE10', 'Dias_Internacion', 'Municipio'
        ]
        if 'Departamento' in consolidado_final.columns:
            column_order_rips.append('Departamento')
        
        column_order_rips = [col for col in column_order_rips if col in consolidado_final.columns]
        
        safe_save_excel(consolidado_final, ruta_salida_rips, column_order_rips)
        print(f"✓ Archivo consolidado guardado en: {ruta_salida_rips}")
        print(f"Total registros: {len(consolidado_final):,}")
        
        ruta_salida_hosvital = rutas['consolidado'] / 'consolidado_hosvital.xlsx'
        safe_save_excel(hosvital_consolidado_final, ruta_salida_hosvital)
        print(f"✓ Archivo consolidado HOSVITAL guardado en: {ruta_salida_hosvital}")
        
        print("\n" + "="*50)
        print("Procesamiento completado exitosamente!")
        print("="*50)
        
    except Exception as e:
        print(f"Error durante el procesamiento: {str(e)}")
        raise

if __name__ == "__main__":
    procesar_rips() 