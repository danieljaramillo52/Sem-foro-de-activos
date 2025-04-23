# Manual de usuario automatización Semáforos. 

## Indicaciones previas de parametrización adicionales. 
Análogamente la proceso orignal de semaforo de activos se hará uso del IDLE de python para la ejecución de la automatización. 

Antes de llevar a cabo la ejecución debemos parámetrizar 3 aspectos fundamentales, únicamente relacionados con la ejecución del proceso.

### 1. Estrtegias de activos.
![Indicaciones de ejecucion](Img_Readme/Meses_a_actualizar_db.png)

Contiene un diccionario con todas los estrategias para los activos. Se deben diligenciar los nombres de las posibles estrategias que pueden llegar a ser variables. 

Para consultar las estrategias activas hacemos lo siguiente: 

  1. Nos dirigimos al archivo de driver de estrategias. Ubicado en la ruta.  **Semaforo/Insumos/Drivers/Drivers.xlsx**

  2. Ingresamos a la hoja llamda "Activos y Estrategias" 

  3. Vamos a la columna ESTRATEGIA 

  4. Filtramos los elementos de esta columna. Así
   - Selecionamos la columna estrategia 
   - Vamos a la barra de herramientas Seleccionamos "Datos" y luego "Filtro" 

![Filtro_Estrategias](Img_Readme/Filtro_estrategias.png?raw=true)
    
- Consultando el filtro extraemos la lista de estrategias actuales en el driver. 

![Filtro_Estrategias](Img_Readme/Estrategias_posibles.png?raw=True)

En este caso podemos ver que las estrategias actuales son: 
    
    - Snackermanía 1.0
    - Neverizate 1.0
    - Snackermanía 2.0 DECOM
    - Snackermanía 2.0
    - Puestos de pago
    
  
Ahora vamos a actualizar las estrategias tanto en el archivo de configuración como en las columnas finales. 

Ingresamos a (config.yml) 
  
1. Abrimos (Notepad++)

![Ingreso_Notepad++](Img_Readme/Ingreso_Notepad++.png?raw=true)
 
2. Abrimos el archivo de configuración

![Abrir_config++](Img_Readme/Open_config.png?raw=true)

3. Seleccionamos el archivo de configuración. 

![Select_config++](Img_Readme/Select_config.png?raw=true)

4. Ventana de configuración para parametrizar. 

![config_estrategias++](Img_Readme/config_estrategias.png?raw=true)

    Tomando la lista de campañas que filtramos en los drivers, podemos actualizar las estrategias presentes que nos interesen, tanto borrar como agregar estrategias. Por defecto agregaremos ademas =>  : col_comas

#### Caso 1. 
  Si queremos eliminar estrategias: 

  1. Seleccionamos la linea completa eliminar y simplmente la borramos. 

  ![select_estrategias1++](Img_Readme/select_estrategia1.png?raw=true)

  2. Guardamos los cambios. 

  ![select_estrategias2++](Img_Readme/select_estrategia2.png?raw=true)

  3. Buscamos dentro del documento "orden_final_cols" (Ctrl + F) como se indico anteriormente y borramos la misma estrategia que eliminamos anteriormente. En este caso, habiamos eliminado "Snakermanía 2.0 DECOM"
  por lo tanto debemos eliminar las lineas: 

    - "Snakermanía 2.0 DECOM" : "Snakermanía 2.0 DECOM"
    - "Estrg_Snakermanía 2.0 DECOM" : "Estrg_Snakermanía 2.0 DECOM"

  Podemos verlo aquí:
  
  ![select_estrategias8++](Img_Readme/select_estrategia8.png?raw=true)

  Al eliminar obtenemos. 

  ![select_estrategias8++](Img_Readme/select_estrategia8.png?raw=true)

  Guardamos los cambios como se indicó.  (Disco de guardado en la parte superior.)
  
#### Caso2. 
  Si queremos agregar estrategias: 

  1. Escribimos el nombre de la estrategia bajo el formato:
    "Nueva_Estrategia" : col_comas
    
  Así si fueramos a agregar una nueva estrategia por ejemplo:
      "Neverizate 2.0"

![select_estrategias3++](Img_Readme/select_estrategia3.png?raw=true)

Podemos cer el valor agregado. Ahora debemos agregarlo al final del documento buscamos la clave.  
**orden_final_cols**

Y debemos agregar esta nueva estrategia como una linea nueva así: 

  "Neverizate 2.0" : "Neverizate 2.0"

Como lo vemos en la siguiente imagen.

1. Dentro de la aplicación de Notepad++ podemos usar **<font color="red">(Ctrl + F)</font>** para abrir el menú de busqueda y encontrar la clave.  **orden_final_cols**

![select_estrategias4++](Img_Readme/select_estrategia4.png?raw=true)

Procedemos a buscar 

![select_estrategias5++](Img_Readme/select_estrategia5.png?raw=true)

Dentro de orden_final_cols vamos a agregar dos lineas nuevas. 

**Ejemplo:** 
Si la estratgia nueva es **Neverizate 2.0**, (que hemos agregado en la parte superior); entonces agregaremos las dos siguientes lineas. 

La estrategia nuevamente de esta forma. 

  - "Neverizate 2.0" : "Neverizate  2.0" 


**Nota:** Además de la estrategia en cuestion, se agrega la linea 
"Estrg_nueva_estrategia" :"Estrg_nueva_estrategia". 
Por defecto podemos notar que corresponde a la estrategia pero con el prefijo **Estrg_** así:

  - "Estrg_Neverizate 2.0" : "Estrg_Neverizate 2.0"

Buscando en orden_final_cols , nos encontraremos las estrategias en cuestión aquí. 

![select_estrategias6++](Img_Readme/select_estrategia6.png?raw=true)

Podemos ver todas las estrategias. Primero con sus nombres, y luego repetidas con el subfijo  **Estrg_**

Al final de las estrategias es que agregaremos la nueva estrategia. 

  - "Neverizate 2.0" : "Neverizate  2.0"  

Al final de las estrategias con subfijos también agregaremos el cambio. 

  - "Estrg_Neverizate 2.0" : "Estrg_Neverizate 2.0"

Finalmente tendremos.

![select_estrategias7++](Img_Readme/select_estrategia7.png?raw=true)

Y las estrategias agregadas. 
Procedemos a aguardar los cambios como en pasos anteriores. 

(Los puntos azules no deben aparecer al agregar las estrategias, solo estan con el objetivo de resaltar.)


----

# Indicaciones de Ejecución Proceso de Semáformafors 

![Indicaciones de ejecucion](Img_Readme/Indicaciones_ejecucion1.png?raw=true)

![Indicaciones de ejecucion](Img_Readme/Indicaciones_ejecucion2.png?raw=true)


![Indicaciones de ejecucion](Img_Readme/Indicaciones_ejecucion3.png?raw=true)

Esto nos llevara al explorador de archivos y debemos ir a la carpeta donde tenemos el archivo + todos los insumos que se necesitan.

Abrimos el archivo main.py dentro de la carpeta Scripts 

![Indicaciones de ejecucion](Img_Readme/Carpeta_scripts.png?raw=true)


Esto nos llevara al explorador de archivos y debemos ir a la carpeta donde tenemos el archivo + todos los insumos que se necesitan.

**Dentro de la carpeta Scripts**

![Indicaciones de ejecucion](Img_Readme/Archivo_main.png)

Abrimos (Damos/click) al archivo main.py, luego obtendremos la siguiente ventana. 

![Indicaciones de ejecucion](Img_Readme/Run_archivo_main.png)

Debemos verificar que en la parte superior nos indique main.py. Luego, para ejecutar el proceso nos vamos a la pestalla superior "Run" y dando click en la opción "Run Module" Y no aparecerá la siguiente instrucción. 

![Indicaciones de ejecucion](Img_Readme/Instrucción_inicial.png)

**"OJO" Por defecto debemos escribir siempre si, como vemos en la siguiente imagen, y luego presionar la tecla Enter**


Así inicia la ejecución del proceso.

![Indicaciones de ejecucion](Img_Readme/Inicio_Ejecución.png?raw=true)

El proceso empieza a ejecutarse, a procesar los archivos de la automatización, debemos esperar hasta que nos pida el mes actual que nos interesa para la automatización.

Procedemos a ingresar el mes actual que estamos corriendo. 

![Indicaciones de ejecucion](Img_Readme/Ingreso_mes.png?raw=true)

Presionamos Enter, y solo queda esperar que ejecute el resto del proceso. 

**El proceso anterior al finalizar mostrará el tiempo final y el simbolo: >>> así:

![Proceso_finalizado](Img_Readme/Proceso_finalizado.png)

Proceso finalizado. 