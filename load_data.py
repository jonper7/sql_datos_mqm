import pandas as pd
import psycopg2
from psycopg2 import extras
import os
import numpy as np
import sys
import tkinter as tk
from tkinter import ttk, messagebox
from threading import Thread
import time
import traceback

# Configuración
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'JPBase',
    'user': 'jonper',
    'password': 'jonper'
}
CARPETA = 'datos_mqm'
ARCHIVO = 'datos.xlsx'
SHEET_NAME = 'datos'
SCHEMA_NAME = 'M_FISICO'
TABLE_NAME = 'mqm_calidad_agua'

def limpiar_celda(val):
    if pd.isna(val):
        return None
    
    if isinstance(val, str):
        val_limpio = val.strip()
        if val_limpio == '' or val_limpio.upper() == 'NAN':
            return None
        try:
            return float(val_limpio.replace(',', '.'))
        except:
            return val_limpio
    
    return val

def actualizar_progreso(valor, mensaje):
    """Actualiza la barra de progreso y el mensaje"""
    try:
        progress_bar['value'] = valor
        label_progreso.config(text=mensaje)
        ventana.update_idletasks()
    except:
        pass

def mostrar_confirmacion():
    """Muestra ventana de confirmación antes de cargar"""
    respuesta = messagebox.askyesno(
        "Confirmar carga",
        f"¿Deseas cargar los datos?\n\n"
        f"📁 Archivo: {CARPETA}/{ARCHIVO}\n"
        f"📊 Hoja: {SHEET_NAME}\n"
        f"🗄️ Destino: {SCHEMA_NAME}.{TABLE_NAME}\n\n"
        f"Se insertarán los datos en la base de datos."
    )
    
    if respuesta:
        cargar_en_hilo()

def cargar_datos():
    try:
        # Deshabilitar botones
        btn_cargar.config(state='disabled')
        btn_cancelar.config(state='disabled')
        
        # Resetear barra de progreso
        progress_bar['value'] = 0
        
        # Paso 1: Obtener ruta del archivo
        actualizar_progreso(5, "📁 Localizando archivo...")
        time.sleep(0.5)
        
        if getattr(sys, 'frozen', False):
            directorio_base = os.path.dirname(sys.executable)
        else:
            directorio_base = os.path.dirname(os.path.abspath(__file__))
        
        ruta_archivo = os.path.join(directorio_base, CARPETA, ARCHIVO)
        
        if not os.path.exists(ruta_archivo):
            raise FileNotFoundError(f"No se encuentra: {ruta_archivo}")
        
        actualizar_progreso(10, "✓ Archivo encontrado")
        time.sleep(0.3)
        
        # Paso 2: Conectar a PostgreSQL
        actualizar_progreso(20, "🔌 Conectando a PostgreSQL...")
        time.sleep(0.5)
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        actualizar_progreso(30, "✓ Conexión establecida")
        time.sleep(0.3)
        
        # Paso 3: Leer Excel
        actualizar_progreso(35, "📊 Leyendo archivo Excel...")
        time.sleep(0.5)
        df = pd.read_excel(ruta_archivo, sheet_name=SHEET_NAME, keep_default_na=True)
        
        total_filas = len(df)
        actualizar_progreso(50, f"✓ {total_filas} filas encontradas")
        time.sleep(0.5)
        
        # Paso 4: Limpiar datos
        actualizar_progreso(55, "🧹 Limpiando datos...")
        time.sleep(0.5)
        datos = [[limpiar_celda(val) for val in row] for row in df.values.tolist()]
        actualizar_progreso(65, "✓ Datos limpiados")
        time.sleep(0.3)
        
        # Paso 5: Preparar inserción
        actualizar_progreso(70, "⚙️ Preparando inserción...")
        time.sleep(0.3)
        columnas = ', '.join([f'"{col}"' for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f'INSERT INTO "{SCHEMA_NAME}"."{TABLE_NAME}" ({columnas}) VALUES ({placeholders})'
        
        # Paso 6: Insertar datos
        actualizar_progreso(75, f"⬆️ Insertando {total_filas} filas...")
        time.sleep(0.5)
        extras.execute_batch(cursor, insert_query, datos, page_size=1000)
        conn.commit()
        actualizar_progreso(90, "✓ Datos insertados")
        time.sleep(0.3)
        
        # Paso 7: Finalizar
        actualizar_progreso(95, "✓ Cerrando conexión...")
        time.sleep(0.3)
        cursor.close()
        conn.close()
        
        # Completado
        actualizar_progreso(100, f"✅ ¡Completado! {total_filas} filas insertadas")
        
        messagebox.showinfo(
            "✓ Proceso completado", 
            f"✅ Carga exitosa\n\n"
            f"📊 Filas insertadas: {total_filas}\n"
            f"🗄️ Tabla: {SCHEMA_NAME}.{TABLE_NAME}\n\n"
            f"Los datos se han cargado correctamente."
        )
        
    except FileNotFoundError as e:
        actualizar_progreso(0, "❌ Archivo no encontrado")
        messagebox.showerror(
            "Error - Archivo no encontrado", 
            f"❌ No se encuentra el archivo:\n\n"
            f"📁 Buscado en: {CARPETA}/{ARCHIVO}\n\n"
            f"💡 Asegúrate de que la carpeta '{CARPETA}' esté\n"
            f"junto al ejecutable y contenga el archivo '{ARCHIVO}'."
        )
    
    except psycopg2.OperationalError as e:
        actualizar_progreso(0, "❌ Error de conexión")
        messagebox.showerror(
            "Error - No se puede conectar", 
            f"❌ No se puede conectar a PostgreSQL:\n\n"
            f"🔌 Host: {DB_CONFIG['host']}\n"
            f"🔌 Puerto: {DB_CONFIG['port']}\n"
            f"🗄️ Base de datos: {DB_CONFIG['database']}\n\n"
            f"💡 Verifica que PostgreSQL esté corriendo\n"
            f"y que las credenciales sean correctas.\n\n"
            f"Error: {str(e)}"
        )
    
    except psycopg2.Error as e:
        actualizar_progreso(0, "❌ Error de base de datos")
        messagebox.showerror(
            "Error - PostgreSQL", 
            f"❌ Error al insertar datos:\n\n{str(e)}"
        )
    
    except Exception as e:
        actualizar_progreso(0, "❌ Error inesperado")
        messagebox.showerror(
            "Error inesperado", 
            f"❌ Error:\n\n{str(e)}\n\n{traceback.format_exc()}"
        )
    
    finally:
        # Rehabilitar botones
        btn_cargar.config(state='normal')
        btn_cancelar.config(state='normal')

def cargar_en_hilo():
    """Ejecuta la carga en un hilo separado"""
    hilo = Thread(target=cargar_datos, daemon=True)
    hilo.start()

def cerrar_ventana():
    """Cierra la aplicación con confirmación"""
    if messagebox.askokcancel("Salir", "¿Deseas cerrar la aplicación?"):
        ventana.destroy()

# ==================== MAIN ====================
def main():
    global ventana, frame_main, btn_cargar, btn_cancelar, progress_bar, label_progreso
    
    try:
        # Crear ventana principal
        ventana = tk.Tk()
        ventana.title("Carga de Datos - MQM Calidad de Agua")
        ventana.geometry("650x500")
        ventana.resizable(False, False)
        ventana.configure(bg='#f5f6fa')
        ventana.protocol("WM_DELETE_WINDOW", cerrar_ventana)
        
        # Centrar ventana
        ventana.update_idletasks()
        ancho = ventana.winfo_width()
        alto = ventana.winfo_height()
        x = (ventana.winfo_screenwidth() // 2) - (ancho // 2)
        y = (ventana.winfo_screenheight() // 2) - (alto // 2)
        ventana.geometry(f'{ancho}x{alto}+{x}+{y}')
        
        # Frame principal
        frame_main = tk.Frame(ventana, bg='#f5f6fa', padx=40, pady=30)
        frame_main.pack(expand=True, fill='both')
        
        # ===== ENCABEZADO =====
        frame_header = tk.Frame(frame_main, bg='#2c3e50', relief='flat')
        frame_header.pack(fill='x', pady=(0, 25))
        
        label_titulo = tk.Label(
            frame_header,
            text="🗄️ CARGA DE DATOS A SQL",
            font=("Arial", 14, "bold"),
            bg='#2c3e50',
            fg='white',
            pady=15
        )
        label_titulo.pack()
        
        # ===== INFORMACIÓN =====
        frame_info = tk.Frame(frame_main, bg='#ffffff', relief='solid', borderwidth=1)
        frame_info.pack(pady=15, fill='x')
        
        label_info_titulo = tk.Label(
            frame_info,
            text="📋 Información de Carga",
            font=("Arial", 11, "bold"),
            bg='#ffffff',
            fg='#2c3e50',
            anchor='w'
        )
        label_info_titulo.pack(fill='x', padx=20, pady=(15, 5))
        
        info_text = f"""
📁  Archivo:  {CARPETA}/{ARCHIVO}
📊  Hoja:     {SHEET_NAME}
🗄️  Destino:  {SCHEMA_NAME}.{TABLE_NAME}
"""
        
        label_info = tk.Label(
            frame_info,
            text=info_text,
            font=("Courier New", 10),
            bg='#ffffff',
            fg='#34495e',
            justify='left',
            anchor='w'
        )
        label_info.pack(fill='x', padx=20, pady=(5, 15))
        
        # ===== FRAME DE BOTONES =====
        frame_botones = tk.Frame(frame_main, bg='#f5f6fa')
        frame_botones.pack(pady=20)
        
        # Botón CARGAR
        btn_cargar = tk.Button(
            frame_botones,
            text="⬆️  CARGAR DATOS",
            command=mostrar_confirmacion,
            font=("Arial", 12, "bold"),
            bg='#27ae60',
            fg='white',
            width=18,
            height=2,
            cursor='hand2',
            relief='flat',
            activebackground='#229954',
            activeforeground='white',
            borderwidth=0
        )
        btn_cargar.pack(side='left', padx=10)
        
        # Botón CANCELAR
        btn_cancelar = tk.Button(
            frame_botones,
            text="❌  CANCELAR",
            command=cerrar_ventana,
            font=("Arial", 12, "bold"),
            bg='#e74c3c',
            fg='white',
            width=18,
            height=2,
            cursor='hand2',
            relief='flat',
            activebackground='#c0392b',
            activeforeground='white',
            borderwidth=0
        )
        btn_cancelar.pack(side='left', padx=10)
        
        # ===== BARRA DE PROGRESO =====
        frame_progreso_container = tk.Frame(frame_main, bg='#f5f6fa')
        frame_progreso_container.pack(fill='x', pady=15)
        
        label_progreso_titulo = tk.Label(
            frame_progreso_container,
            text="Progreso:",
            font=("Arial", 10, "bold"),
            bg='#f5f6fa',
            fg='#2c3e50'
        )
        label_progreso_titulo.pack(anchor='w')
        
        progress_bar = ttk.Progressbar(
            frame_progreso_container,
            length=570,
            mode='determinate',
            maximum=100
        )
        progress_bar.pack(pady=5)
        
        # ===== LABEL DE ESTADO =====
        label_progreso = tk.Label(
            frame_main,
            text="Listo para cargar datos",
            font=("Arial", 10),
            bg='#f5f6fa',
            fg='#7f8c8d'
        )
        label_progreso.pack(pady=5)
        
        # ===== ESTILO DE LA BARRA DE PROGRESO =====
        style = ttk.Style()
        style.theme_use('clam')
        style.configure(
            "TProgressbar",
            thickness=30,
            troughcolor='#ecf0f1',
            bordercolor='#bdc3c7',
            background='#3498db',
            lightcolor='#5dade2',
            darkcolor='#2980b9'
        )
        
        # ===== PIE DE PÁGINA =====
        label_footer = tk.Label(
            frame_main,
            text="v1.0 - Sistema de Carga MQM",
            font=("Arial", 8),
            bg='#f5f6fa',
            fg='#95a5a6'
        )
        label_footer.pack(side='bottom', pady=(10, 0))
        
        # Iniciar aplicación
        ventana.mainloop()
        
    except Exception as e:
        # Si falla la interfaz, mostrar error
        error_ventana = tk.Tk()
        error_ventana.withdraw()
        messagebox.showerror(
            "Error al iniciar",
            f"Error al crear la interfaz:\n\n{str(e)}\n\n{traceback.format_exc()}"
        )
        error_ventana.destroy()

if __name__ == "__main__":
    main()