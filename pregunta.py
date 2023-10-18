"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd

def ingest_data():
    column_names = ['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave', 'principales_palabras_clave']
    column_widths = [5, 7, 19, 100]

    df = pd.read_fwf('clusters_report.txt', widths=column_widths, names=column_names, skiprows=4)

    df_2 = df.copy()

    df_2["grupo"] = df_2["cluster"].ffill().bfill().astype(int)
    df_2 = df_2.groupby("grupo").agg({
        "cluster": "first",
        "cantidad_de_palabras_clave": "first",
        "porcentaje_de_palabras_clave": "first",
        "principales_palabras_clave": lambda x: " ".join(" ".join(word.split()) for word in x)
    }).reset_index(drop=True)

    df_2["principales_palabras_clave"] = df_2["principales_palabras_clave"].str.replace(".", "", regex=False)
    df_2["porcentaje_de_palabras_clave"] = df_2["porcentaje_de_palabras_clave"].str.replace("%", "", regex=False)
    df_2["porcentaje_de_palabras_clave"] = df_2["porcentaje_de_palabras_clave"].str.replace(",", ".", regex=False)
    df_2["porcentaje_de_palabras_clave"] = df_2["porcentaje_de_palabras_clave"].astype(float)
    df_2["cantidad_de_palabras_clave"] = df_2["cantidad_de_palabras_clave"].astype(int)
    df_2["cluster"] = df_2["cluster"].astype(int)
    df_2.columns = df_2.columns.str.replace(' ', '_', regex=False)

    # Guardar la salida en un archivo CSV
    df_2.to_csv('output2.csv', index=False)

    return df_2