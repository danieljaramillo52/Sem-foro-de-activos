# Readme proyecto de semáforos.
# Tabla de Contenido

[introduccion](#Introduccion)
   1.1 [Glosario](#Glosario-de-terminos)
   1.2 [maestras](#Lista-de-Insumos-para-la-automatizacion-de-Comercial-Nutresa)
   1.3 [Objetivo](#objetivo-de-la-automatizacion)
   
2.0 [Archivos_necesarios_para_la_automatizacion](#Lista-archivos-necesarios-para-la-automatización)
   2.1 [maestras_de_activos](#maestras_de_activos)
   2.1 [Universos_de_clientes](#Universos_de_clientes)
   2.1 [Archivos_ventas](#Archivos_ventas_de_clientes)
   2.1 [maestras_de_clientes_inactivos](#maestras_de_clientes_inactivos)
    2.1.1 [Recomendaciones_archivos_de_insumos.](#    2.1.1 [Recomendaciones_archivos_de_insumos.](#Recomendaciones_para_los_archivos_en_Insumos.))
   2.2[Drivers](#Drivers_necesarios_para_la_automatización)
    2.2.1[Drivers](#Drivers.xlsx)
        2.2.1.1[Hoja: Activos y Cargues](#Activos_y_Cargues)
        2.2.1.2[Hoja: Activos y Estrategias](#Activos_y_Estrategias)    
    2.2.2[Driver Neveras en Garantía](#driver_Neveras_en_Garantía)
    2.2.3["Driver Neveras en Mantenimiento](#driver_Neveras_en_Mantenimiento)
    2.2.2[Recomendaciones y obligaciones para la manipulación de los Drivers.](#Recomendaciones-y-obligaciones-drivers)
    2.1.2 [Ubicacion_de_los_drivers.](#ubicacion-de-los-drivers)
   2.3.1[Estructura_archivos_insumos](#estructura-de-los_archivos_de_insumos_y_drivers)
   2.3.2[Estructura_Drivers](#estructura-de-los-drivers-de-información)
   2.3.3["Estructura_y_desglose_resumen](#estructura-y-desglose-resumen)

3. [Archivo_config.yml](#archivo-configyml)
   2.2 [Visualizaciones](#Visualizaciones-del-archivo-(config_yml))
   3.3 [Parametrizaciones_posibles](#parámetrizaciones-posibles)

4. [Resultado_Final](#Resultado_final)

5. [Responsables](#responsables)

6. [Manual_de_usuario](#enlace-al-manual-de-usuario)


# Proyecto de semaforo de activos. 

## Introducción
Este manual contiene toda la información necesaria para el buen uso del asistente del proceso "Automatización de  semaforos de activos". Además, se incluye una descripción detallada de archivos, procedimientos e instrucciones sobre el contenido del ejecutable y la estructura de los archivos finales, entre otros.

## Glosario de terminos

| **Término** | **Definición** |
|-------------|----------------|
| **maestras_de_activos** | Las maestras de activos son las fuentes de información principales, contienen la información de activos relacionada para el proceso de Semáforos.  En esencia se refieren a archivos del tipo xlsx o csv, que seránp rocesados y cargados en memoria con diferentes funciones. Se divide en 2 categorías/ 2 archivos : maestra_activos_sap (Referencia a activos para clientes de la Directa)  y  maestra_activos_indirecta. (Referencia a actvios para clientes de la Indirecta). Cada maestra contiene un tipo especifico de clientes de CN, y a su vez; a cada maestra se le hacen modificaciones y transformaciones para prepararlas antes de consolidarlas como parte final del documento de Semaforos.
| **Drivers** | En este contexto de trabajo, un driver tiene un significado similar a una maestra. Son las fuentes de información adicionales, contienen toda la información de relacionada el Semaforo. En esencia, se refieren a archivos del tipo xlsx o csv, que serán procesados y cargados en memoria con diferentes funciones. Estos son los archivos auxiliares, modificables, y parametrizables que sirven para modificar principalmente los datos, más no las estructuras de las maestras / Universos / o del informe final|
| **Semáforo de activos** | El proceso de Semáforo de activos es un asistente que utiliza información de múltiples fuentes de la compañía Comercial Nutresa. De los dos tipos de atención conocidos (Directa e Indirecta). Su propisito es determinar la medición por las cuales los actvios de compañania pueden ser utilizados. |
| **Activos_comercial** | También llamados activos, son todos los elementos utilidaos por comercial nutresa, auxiliares, entregados a clientes de la Directa y la Indirecta. En base a criterios determinados intermanete por los dueños del proceso en base al resultado de lo vislumbrado por el informe. Base_semaforo_activos.xlsx 

## Objetivo de la automatización
El objetivo se centra en generar un reporte de llamado semaforo de activos comercial nutresa. La automatización provee la información necesaria de insumo, para generar este reporte. Se utilizan los archivos. 

## Lista archivos necesarios para la automatización
      - maestra_clientes_inactivos_indirecta
      - maestra_clientes_inactivos_directa
      - Universo_de_clientes_directa 
      - Universo_de_clientes_indirecta
      - Ventas_muebles_snakeros
      - Ventas_neveras_de_convservacion
      - Maestra_activos_SAP
      - Maestra_activos_Indirecta


## maestras_de_activos
Archivos que contienen todos los activos comerciales que CN, provee a diferentes clientes según criterios de selección internos.  Se divide en dos archivos.

      - Maestra_activos_SAP
      - Maestra_activos_Indirecta

Que corresponden a los dos modelos de atención Directa e indirecta respectivamente.

#### <font color=red>**Maestra_activos_SAP,xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Activos_SAP
- **Driver necesario:**  (NP) 
  - Drivers.xlxs  / Neveras en Garantía.xlsx / Neveras en Mantenimiento.xlsx
- **Insumo generado:** (NP)
  -  Activos_SAP ( Modificación del mismo.)
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Maestra_activos_SAP](Img_Readme/Maestra_activos_sap.png?raw=true)


#### <font color=red>**Maestra_activos_Indirecta,xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Activos_Indirecta
- **Driver necesario:**  (NP) 
  - Drivers.xlxs  / Neveras en Garantía.xlsx / Neveras en Mantenimiento.xlsx
- **Insumo generado:** (NP)
  -  Activos_Indirecta ( Modificación del mismo.)
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Maestra_activos_Indirecta](Img_Readme/Maestra_activos_Indir.png?raw=true)

### Ubicación y carpeta maestras de activos.
![Ruta_maestras_activos](Img_Readme/Ruta_maestras_activos.png?raw=true)

*Nota: Las maestras de activos e encuentran directamente dentro de la carpeta de Insumos 

---

## Archivos_ventas_de_clientes

Archivos que contiene la ventas de los clientes de comercial nutresa. Divididos por Puestos de Pago / Snackeros / Neveras. Se utilizan los siguientes 2 archivos.

      - Ventas_muebles_snakeros
      - Ventas_neveras_de_convservacion

Nota: El archivo de Ventas_muebles_snakeros contiene los clientes con (Muebles Snackeros / Puestos de pago. Y así se manejan sus vtas.)

#### <font color=red>**Ventas_Neveras_de_Conservación.xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Informe 1
- **Driver necesario:**  (NP) 
  Ninguno 
- **Insumo generado:** (NP)
  -  Informe 1 ( Modificación del mismo.)
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Venta_Neveras_de_conservación](Img_Readme/Ventas_Neveras_de_Conservación.png?raw=true)

#### <font color=red>**Ventas_Muebles_Snackeros.xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Consolidado
- **Driver necesario:**  (NP) 
  - Ninguno
- **Insumo generado:** (NP)
  -  Consolidado ( Modificación del mismo.)
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Ventas_Muebles_Snackeros](Img_Readme/Ventas_Muebles_Snackeros.png?raw=true)


### Ubicación Y Carpeta Archivos de Ventas.
![Ruta_Archivos_Ventas](Img_Readme/Ruta_archivos_ventas.png?raw=true)

--- 

## Universos_de_clientes

Archivos que contiene los clientes tanto de la directa como de la indirecta y que tienen el proposito de rellenar o traer la información de clientes que pertenezcan a las maestras de activos. Casi en su totalidad. Se componen nuevamente de 2 archivos.

      - UniversoDirecta.xlsx
      - UniversoInirecta.xlsx

Nota: Los archvios de "Univeros" contienen todos los clientes considerados activos dentro de la estrcutra y organigrama de Comercial Nutresa.Par los clientes inactvios, se usan de referencia las maestras de clientes inactivos. 

#### <font color=red>**UniversoIndirecta.xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Informe 1
- **Driver necesario:**  (NP) 
  Ninguno 
- **Insumo generado:** (NP)
  -  Informe 1 ( Modificación del mismo.)
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Universo_Indirecta](Img_Readme/Universo_Indirecta.png?raw=true)

#### <font color=red>**UniversoDirecta.xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Hoja1
- **Driver necesario:**  (NP) 
  - Ninguno
- **Insumo generado:** (NP)
  -  Hoja1 ( Modificación del mismo.)
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Universo_Directa](Img_Readme/Universo_Directa.png?raw=true)

### Ubicación Y Carpeta Archivos de Ventas.
![Ruta_Archivos_Universo](Img_Readme/Ruta_Insumos_Universo.png?raw=true)

---

## Maestras_de_clientes_inactivos 
Archivos que contiene clientes tanto de la directa como de la indirecta en estatus "Inactivo", tiene el proposito de rellenar o traer la información de clientes que pertenezcan a las maestras de activos y que no hayan sido tratados prevaimente por los Universos de clientes (UniversoDirecta|UniversoIndirecta). Se componen nuevamente de 2 archivos.

      - Maestra Clientes Inactivos Directa.xlsx
      - Maestra Clientes Inactivos Indirecta.xlsx

Nota: Los archvios de "Univeros" contienen todos los clientes considerados activos dentro de la estrcutra y organigrama de Comercial Nutresa.Par los clientes inactvios, se usan de referencia las maestras de clientes inactivos. 


#### <font color=red>**Maestra Clientes Inactivos Directa.xlsx.xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Clientes_Inactivos
- **Driver necesario:**  (NP) 
  Ninguno 
- **Insumo generado:** (NP)
  -  Clientes_Inactivos ( Modificación del mismo.)
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Maestra_Clientes_Inactivos_Directa.xlsx](Img_Readme/Maestra_Clientes_Inactivos_Directa.png?raw=true)

#### <font color=red>**Maestra Clientes Inactivos Indirecta.xlsx.xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Clientes_Inactivos
- **Driver necesario:**  (NP) 
  - Ninguno
- **Insumo generado:** (NP)
  -  Hoja1 ( Modificación del mismo.)
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Maestra Clientes Inactivos Indirecta.xlsx](Img_Readme/Maestra_Clientes_Inactivos_Indirecta.png?raw=true)

### Ubicación Y Carpeta Archivos de Ventas.
![Ruta_Archivos_Universo](Img_Readme/Ruta_Insumos_Universo.png?raw=true)

--- 

## Recomendaciones_para_los_archivos_en_Insumos.

<ul id="requisitos">
  <li>No mover ni sacar ninguna carpeta y/o archivo de la carpeta insumos</li>
  <li>No eliminar ninguno de los archivos anteriormente descritos (solo modificarlos en nombre del archivo y/o nombre de la hoja si se quiere aunque se recomienda trabajar con nombres genéricos)
  <li>Verificar que la extensión siempre sea la misma .xlsx</li>
  <li>No cambiar el nombre a la carpeta anterior (Insumos) Ni cambiar su ubicación</li>
  <li>No cambiar el nombre de las carpetas dentro de ella. (Drivers) / (DB)</li>
  <li>En caso de querer cambiar el nombre (Del archivo o la hoja) cambiar el parámetro (Luego se explicará cómo hacerlo.)</li>
  <li>No agregar a la carpeta archivos adicionales con los mismos nombres que puedan generar conflictos en la automatización.</li>
  <li>Si se desea cambiar el nombre de los archivos se debe hacer parametrizándolos. Proceso que se explicará más adelante en las modificaciones del config.yml.</li>
</ul>

![Visualizalización_carpeta_insumos](Img_readme/Carpeta_insumos.png?raw=true)

![Contenido_Carpeta_insumos](Img_readme/Contenido_Carpeta_Insumos.png?raw=true)



--- 
# Drivers  

(Consultar dfn en Glosario de terminos.)

---
## Drivers_necesarios_para_la_automatización
      - Drivers.xlsx
      - Neveras en Garantía.xlsx
      - Neveras en Mantenimiento.xlsx
  


#### <font color=red>**Drivers.xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Activos y Cargues
  - Activos y Estrategias
  - DRIVER REGIONALES
  - HISTÓRICO TOPES

- **Insumo dependiente:** (NP)
  -  Maestra_activos_SAP.xlsx / Maestra_activos_INDIRECTA.xlsx
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Maestra Clientes Inactivos Indirecta.xlsx](Img_Readme/Drivers.png?raw=true)


#### <font color=red>**Neveras en Garantía.xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Garantía Neveras
- **Insumo dependiente:** (NP)
  -  Maestra_activos_SAP.xlsx / Maestra_activos_INDIRECTA.xlsx
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Neveras_en_Garantia](Img_Readme/Neveras_en_Garantía.png?raw=true)


#### <font color=red>**Neveras en Mantenimiento.xlsx**</font>

- **Hoja necesaria:**  (NP) 
  - Mantenimiento Neveras
- **Insumo dependiente:** (NP)
  -  Maestra_activos_SAP.xlsx / Maestra_activos_INDIRECTA.xlsx
-  **Tipo de archivo:** Archivo de Excel
- **Formato de archivo:** Archivos Dinámicos / No contiene macros
- **Macros necesarias para el proceso:** Ninguna

![Neveras_en_Mantenimiento](Img_Readme/Neveras_en_Mantenimiento.png?raw=true)


### Ubicacion-de-los-drivers
![Ruta_Archivos_Universo](Img_Readme/Ubicación_drivers.png?raw=true)

#### Recomendaciones y obligaciones Drivers.
<ul>
    <li>No mover los archivos de la carpeta "Insumos".</li>
    <li>No eliminar ninguno de los 3 archivos mencionados anteriormente (solo se pueden modificar en nombredelarchivo y/o nombre de la hoja)</li>
    <li>Verificar que la extensión del archivo siempre sea la misma (.xlsx).</li>
    <li>No cambiar el nombre de la carpeta anterior ("Insumos")ni cambiar su ubicación.</li>
    <li>En caso de querer cambiar elnombre del archivo o de la hoja,cambiar el parámetro según las instrucciones proporcionadas.</li>
    <li>No agregar archivos adicionales a la carpeta con los mismos nombres que puedan generar conflictos en la automatización.</li>
    <li>Respetar los nombres de las columnas de cada archivo y mantener la estructura para facilitar la parametrización y reconocimiento fácil.</li>
    <li>Para consultar la estructurade los drivers, se proporcionauna visualización de las columnas utilizadas. Para ver la estructura completa y correcta, consultar "Estructura_insumos_drivers.xlsx" dentro de la carpeta de documentación.</li>
    <li><b>Es importante tener encuenta que puede haber nombres repetidos de columnas en los drivers, y esto no debe cambiarse en ninguna columna, especialmente en el driver_cadenas.</b></li>
    </ul>


## Estructura-de-los_archivos_de_insumos
Las estructuras de los insumos utilizados en la automatización estan descritos en el archivo, dentro de la carpeta documentación Semaforo/Documentación/Estructuras_insumos_drivers.xlsx Consultar Hoja Estructura_archivos_insumos 

![Archivo_Estructuras](Img_Readme/Estructura_insumos_drivers.png?raw=true)


Muestra un comilado de como deben verse los archivos y un fragmento de los datos contenidos en cada columna. 

Similarmente ocurre con los drivers necesarios para la automatización. 
Consultar como se indica en:. **Estructuras_insumos_drivers.xlsx**

### Diccionario de elementos. 
En el archivo Estructuras_insumos_drivers.xlsx, se encuentra la hoja 
**Diccionario_elementos**  una tabla con la estrucutra de nombre de archivo, número de columnas de cada archivo, nombre de las hojas en cada archivo. Parámetros que hay que mantener para garantizar una estructura y el funcionamiento de una automatización. 


## Archivo (config.yml)
![Archivo_cofing](Img_Readme/config_yml.png?raw=true)

* **Tipo de archivo**  Archivo yml (De parámetros)
* **Formato del archivo** (yml) (Formato especial de archivo de texto para parametrizar)

### Visualizaciones del archivo (config_yml) 
`Diferentes visualizaciones del mismo archivo` 

![Ruta_Drivers](Img_Readme/visual_config_VSC.png?raw=true)

![Ruta_Drivers](Img_Readme/visual_config_block_notas.png?raw=true)

![Ruta_Drivers](Img_Readme/visual_config_Notepad++.png?raw=true)

Las anteriores son visualizaciones para trabajar el **config.yml**, presente en la automatización. Dichas visualizaciones corresponden a los programas. 

#### Editor de código (USO NO RECOMENDADO) 

![Visual_VSC](Img_Readme/visual_VSC.png?raw=true)

#### Block de notas (USO NO RECOMENDADO) 

![visual_block_notas](Img_Readme/visual_block_notas.png?raw=true)

#### Notepad++ (USO RECOMENDADO)

![visual_notedpadd++](Img_Readme/visual_Notepad++.png?raw=true)

--- 

### Contenido y estructura (Resumen)

#### Rutas de las Carpetas Insumos/Drivers/Ventas/Resultados
![Ruta_Drivers](Img_Readme/Ruta_archivos_drivers.png?raw=true)

Carpetas que contienen los insumos para correr toda la automatización, y además donde se  pueden consultar los resultados luego de ejecutarla. Así mismo, esta configuración da la   ruta para leer y guardar. **Esta parte no se cambia, no se toca, NO se modifica en  ninguna circunstancia.**

--- 

El archivo de configuración solo debe ser abierto en con el uso de la aplicación **Notepad++** Y se parámetriza todo tal cual en el manual de usuario del proyecto. 

Para los archivos de insumos y los drivers se utilizará también en el archivo Estructuras_insumos_drivers.xlsx La lista de archivos y parámetrizaciones genéricas a comprobar para los mismos.  Consultar en el archivo la hoja Diccionario_Elementos

| Carpeta          | Archivo                      | Nom Hoja 1          | Nom Hoja 2     | Cols parámetro Hoja 1 | Cols parámetro Hoja 2 |
|------------------|------------------------------|----------------------|----------------|-----------------------|-----------------------|
| Drivers          | Drivers.xlsx                 | Activos y Estrategias| Activos y Cargues | 5                     | 9                     |
| Drivers          | Drivers.xlsx                 | HISTÓRICO TÓPES | - | -                     | -                     |
| Drivers          | Drivers.xlsx                 | HISTÓRICO TÓPES | - | -                     | -                     |
| Drivers          | Drivers.xlsx                 | DRIVER REGIONALES | - | -                     | -                     |
| Drivers          | Neveras en Garantía.xlsx     | Garantía Neveras     | -              | 1                     | -                     |
| Drivers          | Neveras en Mantenimiento.xlsx| Mantenimiento Neveras| -              | 1                     | -                     |
| Maestras_inactivos | Maestra Clientes Inactivos Directa.xlsx| Clientes_Inactivos| -              | 19                    | -                     |
| Maestras_inactivos | Maestra Clientes Inactivos Indirecta.xlsx| Clientes_Inactivos| -              | 19                    | -                     |
| Universos        | UniversoDirecta.xlsx         | Hoja1                | -              | 58                    | -                     |
| Universos        | UniversoIndirecta.xlsx       | Hoja1                | -              | 34                    | -                     |
| Ventas           | Ventas_Muebles_Snackeros.xlsx| Consolidado          | -              | 10                    | -                     |
| Ventas           | Ventas_Neveras_de_Conservación.xlsx| Informe 1         | -              | 10                    | -                     |


## Resultado_final

## Historico_Vtas.xlsx
Cada vez que se corre la automatización se generan dos resultados. 

### Caso 1.
En caso de que los archivos y el mes actual del semáforo correspondan a Enero. 
La automatizacion genera por defecto un archivo llamado: **Historico_Vtas.xlsx**

![Historico_de_vtas](Img_Readme/Historico_vtas.png?raw=true)

El archivo anterior recopila un historico de ventas con la doble llave "Cliente" - "TipoActivo" es de estrucutra variable y se genera o actualiza automaticamente con el correr de la automatización. No debe tocarse ni ser modificado. 

El archivo se genera a partir de los archivos de ventas de Neveras / Snakeros. 
Con la combinación de la llave mencionada anteriormente. Puede contener cierta cantidad de meses dependiendo del mes a actualizar. (**Explicación ampliada en el manual de usuario de la automatización.**)


Visualización del historico de vtas. Unciamente con el mes de Enero corrido en la automatización. 

![Visual_Historico_de_vtas_Enero](Img_Readme/Visual_Vtas_ENE.png?raw=true)


### Caso 2.
A medida que se corra el proceso durante el año,  van aumentando las ventas y actualizan los meses con vtas disponible para el reporte de semáforos. Podemos notar que tenemos un historico de Vtas ahora con los clientes actualizados activos para el informe y con más de un mes de vtas. Se actualiza automaticamente el archivo ya existente para el mes de enero.

![Visual_Historico_de_vtas](Img_Readme/Vtas_Visual.png?raw=true)

--- 

## Base_semaforo_activos.xlsx

Resultado final de la automatización Descrita en el archivo de Estructura_archivos_Drivers.xlsx  Estructura de las columnas y archivo final. 

![Base_semaforo_activos.xlsx](Img_Readme/Base_semaforo_Activos.png?raw=true)

Lista de columnas Fijas:

    - Cliente
    - Tipo Activo
    - Denominación objeto
    - Cod Barras
    - Fe.suministro
    - Año_act
    - Mes_act
    - Cód Loc Actual
    - Razón Social
    - Nombre Comercial
    - Cód. JV
    - Jefe de Ventas
    - Código Vendedor Z1
    - Código Vendedor ZA
    - Vendedor ZA
    - Ecom
    - Cód. AC
    - Agente Comercial
    - Canal Trans.
    - Sub Canal Trans.
    - Segmento Trans.
    - Oficina de Ventas
    - Cód. OV
    - Municipio
    - Región
    - Fecha Instalación Ajustada
    - MODELO
    - Venta Acum.
    - Venta $ ENE
    - Venta $ FEB
    - Venta $ MAR
    - Venta $ ABR
    - Venta $ MAY
    - Venta $ JUN
    - Venta $ JUL
    - Venta $ AGO
    - Venta $ SEP
    - Venta $ OCT
    - Venta $ NOV
    - Venta $ DIC
    - PENDIENTE DE VENTA
    - PENDIENTE DE META
    - MESES_DISPONIBLES
    - N.de Activos
    - Estatus_Venta Acum.
    - TOP_VERDE
    - TOP_ROJO
    - Cargue
    - Mantenimiento Nev
    - Garantia Nev
    - __Snackermanía 1.0__
    - __Neverizate 1.0__
    - __Snackermanía 2.0 DECOM__
    - __Snackermanía 2.0__
    - __Puestos de pago__
    - __Estrg_Snackermanía 1.0__
    - __Estrg_Neverizate 1.0__
    - __Estrg_Snackermanía 2.0 DECOM__
    - __Estrg_Snackermanía 2.0__
    - __Estrg_Puestos de pago__
    - Estrategia_Agrupada
    - Cliente Inactivo
    - Estatus_Venta $ ENE
    - Estatus_Venta $ FEB
    - Estatus_Venta $ MAR
    - Estatus_Venta $ ABR
    - Estatus_Venta $ MAY
    - Estatus_Venta $ JUN
    - Estatus_Venta $ JUL
    - Estatus_Venta $ AGO
    - Estatus_Venta $ SEP
    - Estatus_Venta $ OCT
    - Estatus_Venta $ NOV
    - Estatus_Venta $ DIC
    - TIEMPO EN ROJO
    - TIEMPO VENTA CERO

Las columnas que inician y terminan con "__" (En la lista anterior)
son columnas no fijas, variables que se parámetrizan antes de correr la automatización según el mes indicado. Y deben estar presentes en la columna "Estrategia" del Archivo "Drivers.xlsx" en su Hoja "Activos y Cargues"

Consultar Manual de usuario para parámetrización de estas columnas de estrategias.



## Responsables
### Provededor - XpertGroup.
* Daniel jaramillo Bustamante - daniel.jaramillo@xpertgroup.co

### Receptor - Comercial Nutresa.
* **Aréa TI:**
    * Sebastián Caro Aguirre scaro@comercialnutresa.com.co

## Enlace al manual de usuario. 
[Manual de Usuario](ManualDeUsuario.md) 




