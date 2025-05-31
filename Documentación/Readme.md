# 📘 Readme del Proyecto de Semáforos

## 📑 Tabla de Contenido

1. [Introducción](#introducción)  
   1.1 [Glosario](#glosario-de-términos)  
   1.2 [Maestras](#maestras_de_activos)  
   1.3 [Objetivo](#objetivo-de-la-automatización)  

2. [Archivos necesarios para la automatización](#lista-archivos-necesarios-para-la-automatización)  
   2.1 [Maestras de activos](#maestras_de_activos)  
   2.2 [Universos de clientes](#universos_de_clientes)  
   2.3 [Archivos de ventas](#archivos_ventas_de_clientes)  
   2.4 [Maestras de clientes inactivos](#maestras_de_clientes_inactivos)  
   2.5 [Recomendaciones para los archivos en Insumos](#recomendaciones_para_los_archivos_en_insumos)  
   2.6 [Drivers](#drivers_necesarios_para_la_automatización)  
   2.7 [Estructura de insumos y drivers](#estructura-de-los_archivos_de_insumos)  

3. [Archivo config.yml](#archivo-configyml)  
   3.1 [Visualizaciones](#visualizaciones-del-archivo-config_yml)  
   3.2 [Parametrizaciones posibles](#parámetrizaciones-posibles)  

4. [Resultado Final](#resultado_final)  
5. [Responsables](#responsables)  
6. [Manual de Usuario](#enlace-al-manual-de-usuario)  

---

# 🟢 Proyecto de Semáforo de Activos

## 🔍 Introducción
Este manual contiene toda la información necesaria para el buen uso del asistente del proceso **"Automatización de semáforos de activos"**. Además, se incluye una descripción detallada de archivos, procedimientos e instrucciones sobre el ejecutable y la estructura de los archivos finales.

## 📘 Glosario de términos

| **Término** | **Definición** |
|------------|----------------|
| **maestras_de_activos** | Archivos base con datos de activos comerciales, divididos en directa e indirecta. Procesados y consolidados en el semáforo final. |
| **Drivers** | Archivos complementarios que parametrizan el análisis. No modifican la estructura, pero sí los valores usados. |
| **Semáforo de activos** | Sistema de evaluación del uso y estado de activos de Comercial Nutresa. |
| **Activos_comercial** | Elementos físicos usados por clientes de Comercial Nutresa para operaciones comerciales. |

## 🎯 Objetivo de la automatización

Generar automáticamente el reporte **"Semáforo de Activos Comercial Nutresa"**, consolidando insumos de distintas fuentes y automatizando su transformación.

---

## 📂 Lista de archivos necesarios para la automatización

- `maestra_clientes_inactivos_indirecta`  
- `maestra_clientes_inactivos_directa`  
- `Universo_de_clientes_directa`  
- `Universo_de_clientes_indirecta`  
- `Ventas_muebles_snakeros`  
- `Ventas_neveras_de_convservacion`  
- `Maestra_activos_SAP`  
- `Maestra_activos_Indirecta`  

---

## 🗂️ Maestras de activos

Archivos que contienen todos los activos comerciales proporcionados por CN. Se clasifican según el modelo de atención:

- `Maestra_activos_SAP` (Directa)  
- `Maestra_activos_Indirecta` (Indirecta)

### 📄 Maestra_activos_SAP.xlsx
- **Hoja:** `Activos_SAP`  
- **Drivers:** `Drivers.xlsx`, `Neveras en Garantía.xlsx`, `Neveras en Mantenimiento.xlsx`  
- **Insumo generado:** Activos_SAP (modificado)  
- **Tipo/Formato:** Excel dinámico, sin macros  

![Maestra_activos_SAP](Img_Readme/Maestra_activos_sap.png?raw=true)

### 📄 Maestra_activos_Indirecta.xlsx
- **Hoja:** `Activos_Indirecta`  
- **Drivers:** `Drivers.xlsx`, `Neveras en Garantía.xlsx`, `Neveras en Mantenimiento.xlsx`  
- **Insumo generado:** Activos_Indirecta (modificado)  
- **Tipo/Formato:** Excel dinámico, sin macros  

![Maestra_activos_Indirecta](Img_Readme/Maestra_activos_Indir.png?raw=true)

📁 **Ubicación:** Carpeta `Insumos`

![Ruta_maestras_activos](Img_Readme/Ruta_maestras_activos.png?raw=true)

---

## 💰 Archivos de ventas de clientes

Archivos que registran ventas por tipo de activo: Neveras y Muebles Snackeros.

- `Ventas_muebles_snakeros`  
- `Ventas_neveras_de_convservacion`

📝 *Nota: Muebles Snackeros se agrupan como "puestos de pago".*

### 📄 Ventas_Neveras_de_Conservación.xlsx
- **Hoja:** `Informe 1`  
- **Driver necesario:** Ninguno  
- **Insumo generado:** Informe 1 (modificado)  
- **Tipo/Formato:** Excel dinámico, sin macros  

![Venta_Neveras_de_conservación](Img_Readme/Ventas_Neveras_de_Conservación.png?raw=true)

### 📄 Ventas_Muebles_Snackeros.xlsx
- **Hoja:** `Consolidado`  
- **Driver necesario:** Ninguno  
- **Insumo generado:** Consolidado (modificado)  
- **Tipo/Formato:** Excel dinámico, sin macros  

![Ventas_Muebles_Snackeros](Img_Readme/Ventas_Muebles_Snackeros.png?raw=true)

📁 **Ubicación:** Carpeta de archivos de ventas

![Ruta_Archivos_Ventas](Img_Readme/Ruta_archivos_ventas.png?raw=true)

---



--- 
...

---

## 👥 Universos de clientes

Archivos que contienen la base de clientes atendidos por Comercial Nutresa, tanto de la Directa como de la Indirecta. Se usan para validar y complementar los activos asociados a cada cliente.

- `UniversoDirecta.xlsx`  
- `UniversoIndirecta.xlsx`

📝 *Nota: Estos archivos representan los clientes activos. Para clientes inactivos, se usan las maestras correspondientes.*

### 📄 UniversoIndirecta.xlsx
- **Hoja necesaria:** `Informe 1`  
- **Driver necesario:** Ninguno  
- **Insumo generado:** Informe 1 (modificado)  
- **Tipo/Formato:** Excel dinámico, sin macros  

![Universo_Indirecta](Img_Readme/Universo_Indirecta.png?raw=true)

### 📄 UniversoDirecta.xlsx
- **Hoja necesaria:** `Hoja1`  
- **Driver necesario:** Ninguno  
- **Insumo generado:** Hoja1 (modificado)  
- **Tipo/Formato:** Excel dinámico, sin macros  

![Universo_Directa](Img_Readme/Universo_Directa.png?raw=true)

📁 **Ubicación:** Carpeta de archivos de universo

![Ruta_Archivos_Universo](Img_Readme/Ruta_Insumos_Universo.png?raw=true)

---

## 🚫 Maestras de clientes inactivos

Archivos que contienen clientes en estatus "Inactivo" para las atenciones Directa e Indirecta. Sirven para identificar clientes que no aparezcan en los universos pero tengan relación con activos.

- `Maestra Clientes Inactivos Directa.xlsx`  
- `Maestra Clientes Inactivos Indirecta.xlsx`

📝 *Nota: Estas maestras complementan los archivos Universo, aportando contexto sobre clientes no activos que deben excluirse del análisis final.*

### 📄 Maestra Clientes Inactivos Directa.xlsx
- **Hoja necesaria:** `Clientes_Inactivos`  
- **Driver necesario:** Ninguno  
- **Insumo generado:** Clientes_Inactivos (modificado)  
- **Tipo/Formato:** Excel dinámico, sin macros  

![Maestra_Clientes_Inactivos_Directa.xlsx](Img_Readme/Maestra_Clientes_Inactivos_Directa.png?raw=true)

### 📄 Maestra Clientes Inactivos Indirecta.xlsx
- **Hoja necesaria:** `Clientes_Inactivos`  
- **Driver necesario:** Ninguno  
- **Insumo generado:** Clientes_Inactivos (modificado)  
- **Tipo/Formato:** Excel dinámico, sin macros  

![Maestra_Clientes_Inactivos_Indirecta](Img_Readme/Maestra_Clientes_Inactivos_Indirecta.png?raw=true)

📁 **Ubicación:** Carpeta de insumos - Subcarpeta `Maestras_inactivos`

![Ruta_Archivos_Universo](Img_Readme/Ruta_Insumos_Universo.png?raw=true)

---


📦 Recomendaciones para los archivos en Insumos
Para asegurar el correcto funcionamiento del sistema de automatización, es fundamental seguir estas recomendaciones con respecto a la carpeta Insumos y su contenido:

✅ Buenas prácticas
📁 No mover ni sacar archivos o subcarpetas de la carpeta Insumos.

🗑️ No eliminar ninguno de los archivos descritos (pueden modificarse pero no eliminarse).

📝 Mantener la extensión de los archivos como .xlsx.

📌 No cambiar el nombre de la carpeta Insumos ni su ubicación.

📂 No renombrar las subcarpetas internas como Drivers, DB, etc.

🔁 Si se desea cambiar el nombre de un archivo o hoja, esto debe hacerse parametrizando el cambio en el archivo config.yml.

⚠️ No agregar archivos con nombres duplicados que puedan generar conflictos.

🧱 Respetar la estructura de columnas dentro de los archivos: no modificar encabezados, posiciones ni agregar columnas adicionales sin parametrización.



![Visualizalización_carpeta_insumos](Img_readme/Carpeta_insumos.png?raw=true)

![Contenido_Carpeta_insumos](Img_readme/Contenido_Carpeta_Insumos.png?raw=true)



--- 
...

---

## 🛠️ Drivers necesarios para la automatización

Los drivers son archivos auxiliares que contienen información complementaria y parametrizable para el proceso de automatización. Estos deben mantenerse actualizados y con el formato adecuado:

- `Drivers.xlsx`
- `Neveras en Garantía.xlsx`
- `Neveras en Mantenimiento.xlsx`

### 📄 Drivers.xlsx

- **Hojas necesarias:**
  - Activos y Cargues
  - Activos y Estrategias
  - DRIVER REGIONALES
  - HISTÓRICO TOPES

- **Insumos dependientes:** `Maestra_activos_SAP.xlsx`, `Maestra_activos_INDIRECTA.xlsx`
- **Tipo/Formato:** Excel dinámico, sin macros

![Drivers](Img_Readme/Drivers.png?raw=true)

### 📄 Neveras en Garantía.xlsx

- **Hoja necesaria:** Garantía Neveras
- **Insumos dependientes:** `Maestra_activos_SAP.xlsx`, `Maestra_activos_INDIRECTA.xlsx`
- **Tipo/Formato:** Excel dinámico, sin macros

![Neveras_en_Garantia](Img_Readme/Neveras_en_Garantía.png?raw=true)

### 📄 Neveras en Mantenimiento.xlsx

- **Hoja necesaria:** Mantenimiento Neveras
- **Insumos dependientes:** `Maestra_activos_SAP.xlsx`, `Maestra_activos_INDIRECTA.xlsx`
- **Tipo/Formato:** Excel dinámico, sin macros

![Neveras_en_Mantenimiento](Img_Readme/Neveras_en_Mantenimiento.png?raw=true)

📁 **Ubicación de los drivers:** Carpeta `Insumos/Drivers`

![Ruta_Archivos_Universo](Img_Readme/Ubicación_drivers.png?raw=true)

### 📋 Recomendaciones y obligaciones sobre los Drivers

- 📁 No mover los archivos fuera de la carpeta `Insumos`.
- 🗑️ No eliminar los drivers mencionados (pueden modificarse con cuidado).
-  Mantener la extensión `.xlsx`.
-  No cambiar el nombre de la carpeta `Insumos` ni de la subcarpeta `Drivers`.
- 🧾 Si se renombra un archivo o una hoja, debe reflejarse en `config.yml`.
- ⚠️ No agregar archivos con nombres duplicados.
- Respetar las estructuras y nombres de columnas.
-  Evitar duplicidad de nombres en las columnas (especialmente en `driver_cadenas`).

 Para más información sobre estructura y columnas, consultar el archivo:
**`Estructuras_insumos_drivers.xlsx`** hoja **`Diccionario_elementos`** dentro de la carpeta `Documentación`.

![Estructura_insumos_drivers](Img_Readme/Estructura_insumos_drivers.png?raw=true)

---

## 🧱 Estructura de los archivos de insumos

La estructura de los archivos utilizados en la automatización está definida en el archivo `Estructuras_insumos_drivers.xlsx`.

📍 Ubicación: `Semáforo/Documentación/Estructuras_insumos_drivers.xlsx`

- Hoja clave: `Estructura_archivos_insumos`
- Contiene ejemplos de estructuras de columnas, formatos esperados y parámetros obligatorios para funcionamiento.

### 📘 Diccionario de elementos

Hoja: `Diccionario_elementos`  
Incluye una tabla con:
- Nombre de archivo
- Cantidad de columnas por hoja
- Nombre de hojas
- Parámetros críticos

Esto permite parametrizar adecuadamente todos los insumos desde `config.yml`.

---



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





