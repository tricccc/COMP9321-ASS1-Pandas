import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
import numpy as np
import math
import re
import time

studentid = os.path.basename(sys.modules[__name__].__file__)


def log(question, output_df, other):
    print("--------------- {}----------------".format(question))

    if other is not None:
        print(question, other)
    if output_df is not None:
        df = output_df.head(5).copy(True)
        for c in df.columns:
            df[c] = df[c].apply(lambda a: a[:20] if isinstance(a, str) else a)

        df.columns = [a[:10] + "..." for a in df.columns]
        print(df.to_string())


def question_1(exposure, countries):
    """
    :param exposure: the path for the exposure.csv file
    :param countries: the path for the Countries.csv file
    :return: df1
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    df_exposure = pd.read_csv(exposure, encoding='latin-1', sep=';', low_memory=False)
    df_countries = pd.read_csv(countries)
    df_exposure = df_exposure.dropna(subset=['country'], how='any')
    df_exposure.rename(columns={'country': 'Country'}, inplace=True)
    # Because there are lots of countries have name issue, so we need to pre-process the data before merging
    # We use left-merge to filter the country from df_exposure which do not match anything
    # Storing these countries in array different_country_left
    df_merge_left = pd.merge(df_exposure, df_countries, on=['Country'], how='left')
    df_merge_left = df_merge_left.sort_index()
    different_country_left = list(df_merge_left[pd.isnull(df_merge_left['Cities'])]['Country'])
    # Likely, filter the country from df_countries which do not match anything
    # Storing these countries in array different_country_right
    df_merge_right = pd.merge(df_exposure, df_countries, on=['Country'], how='right')
    df_merge_right = df_merge_right.sort_index()
    different_country_right = list(df_merge_right[pd.isnull(df_merge_right['Income classification according to WB'])]['Country'])
    # Then, I compare them manually to create a dictionary
    # According to using Wikipedia, I find these countries' different name
    def process_1(x):
        compare_country_dist = {'Palestine': 'Palestinian Territory', 'United States of America': 'United States',
                                'Congo DR': 'Democratic Republic of the Congo', 'Congo': 'Republic of the Congo',
                                'Korea DPR': 'North Korea', 'Lao PDR': 'Laos', 'Brunei Darussalam': 'Brunei',
                                'Viet Nam': 'Vietnam', 'Eswatini': 'Swaziland', 'Cabo Verde': 'Cape Verde',
                                'Moldova Republic of': 'Moldova',
                                'Russian Federation': 'Russia', 'Korea Republic of': 'South Korea',
                                'North Macedonia': 'Macedonia',
                                'Côte d\'Ivoire': 'Ivory Coast'}
        if x in compare_country_dist:
            return compare_country_dist[x]
        else:
            return x
    # According to the dictionary, renaming these countries
    df_exposure["Country"] = df_exposure["Country"].apply(process_1)
    df1 = pd.merge(df_exposure, df_countries, on=['Country'])
    df1.set_index('Country', inplace=True)
    df1 = df1.sort_index(axis=0, ascending=True)
    #################################################

    log("QUESTION 1", output_df=df1, other=df1.shape)
    return df1


def question_2(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df2
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    df2 = df1.copy()
    new_columns = {'avg_latitude': [], 'avg_longitude': []}
    tmp = df2['Cities'].str.replace('\|\|\|', ',')
    # Split the 'Cities' and calculate the mean of 'Latitude' and 'Longitude'
    new_columns['avg_latitude'] = list(
        tmp.apply(lambda x: (pd.DataFrame(json.loads(s='[' + x + ']')))['Latitude'].mean()))
    new_columns['avg_longitude'] = list(
        tmp.apply(lambda x: (pd.DataFrame(json.loads(s='[' + x + ']')))['Longitude'].mean()))
    # Add these two columns to dataframe
    df2.insert(len(df2.columns), 'avg_latitude', new_columns['avg_latitude'])
    df2.insert(len(df2.columns), 'avg_longitude', new_columns['avg_longitude'])
    #################################################

    log("QUESTION 2", output_df=df2[["avg_latitude", "avg_longitude"]], other=df2.shape)
    return df2


def question_3(df2):
    """
    :param df2: the dataframe created in question 2
    :return: df3
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Calculate distance according to latitude and longitude
    def caculate_distance(lat2, lng2):
        earth_radius = 6373
        lat1 = 30.5928
        lng1 = 114.3055
        lat1 = lat1 * math.pi / 180.0
        lng1 = lng1 * math.pi / 180.0
        lat2 = lat2 * math.pi / 180.0
        lng2 = lng2 * math.pi / 180.0
        d_lat = lat2 - lat1
        d_lng = lng2 - lng1
        distance = 2 * math.asin(
            math.sqrt(math.sin(d_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * (math.sin(d_lng / 2) ** 2)))
        distance = distance * earth_radius
        return distance

    df3 = df2.copy()
    distance = list(df3.apply(lambda x: caculate_distance(x['avg_latitude'], x['avg_longitude']), axis=1))
    df3.insert(len(df3.columns), 'distance_to_Wuhan', distance)
    df3 = df3.sort_values(by='distance_to_Wuhan')
    #################################################

    log("QUESTION 3", output_df=df3[['distance_to_Wuhan']], other=df3.shape)
    return df3


def question_4(df2, continents):
    """
    :param df2: the dataframe created in question 2
    :param continents: the path for the Countries-Continents.csv file
    :return: df4
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    df_continents = pd.read_csv(continents)
    # Because there are lots of countries have name issue, so we need to pre-process the data before merging
    # We use left-merge to filter the country from df2 which do not match anything in df_exposure
    # Storing these countries in array different_country_left
    df_merge_left = pd.merge(df2, df_continents, on='Country', how='left')
    df_merge_left = df_merge_left.sort_index()
    different_country_left = list(df_merge_left[pd.isnull(df_merge_left['Continent'])]['Country'])
    # Likely, filter the country from df_continent which do not match anything
    # Storing these countries in array different_country_right
    df_merge_right = pd.merge(df2, df_continents, on='Country', how='right')
    df_merge_right = df_merge_right.sort_index()
    different_country_right = list(df_merge_right[pd.isnull(df_merge_right['Income classification according to WB'])]['Country'])
    # Then, I compare them manually to create a dictionary
    # According to using Wikipedia, I find these countries' different name
    def process_4(x):
        compare_country_dist = {'Burkina': 'Burkina Faso', 'Congo': 'Republic of the Congo', 'Burma (Myanmar)': 'Burma',
                                'Congo, Democratic Republic of': 'Democratic Republic of the Congo',
                                'East Timor': 'Timor-Leste',
                                'Korea, North': 'North Korea', 'Korea, South': 'South Korea',
                                'Russian Federation': 'Russia',
                                'CZ': 'Czech Republic', 'US': 'United States'
                                }
        if x in compare_country_dist:
            return compare_country_dist[x]
        else:
            return x
    # According to the dictionary, renaming these countries
    df_continents["Country"] = df_continents["Country"].apply(process_4)
    # Then, starting merging these tow dataframe
    df_merge = pd.merge(df2, df_continents, on='Country')
    df_merge = df_merge[['Country', 'Covid_19_Economic_exposure_index', 'Continent']]
    # Filter data without value
    df_merge['Covid_19_Economic_exposure_index'] = df_merge['Covid_19_Economic_exposure_index'].str.replace(',', '.')
    df_merge = df_merge[~ df_merge['Covid_19_Economic_exposure_index'].str.contains('x')]
    df_merge[['Covid_19_Economic_exposure_index']] = df_merge[['Covid_19_Economic_exposure_index']].apply(
        pd.to_numeric)
    # Grouping dataframe by 'Continent' and calculating the mean of 'Covid_19_Economic_exposure_index'
    df_groupby_continent = df_merge.groupby(['Continent'], as_index=False)
    df_merge = df_groupby_continent['Covid_19_Economic_exposure_index'].mean()
    df_merge.rename(columns={'Covid_19_Economic_exposure_index': 'average_covid_19_Economic_exposure_index'},
                    inplace=True)
    df_merge.set_index('Continent', inplace=True)
    df4 = df_merge.sort_values(by='average_covid_19_Economic_exposure_index')
    #################################################

    log("QUESTION 4", output_df=df4, other=df4.shape)
    return df4


def question_5(df2):
    """
    :param df2: the dataframe created in question 2
    :return: cities_lst
            Data Type: list
            Please read the assignment specs to know how to create the output dataframe
    """
    #################################################
    df5 = df2
    # Filter data without value
    df5 = df5[~ df5['Net_ODA_received_perc_of_GNI'].str.contains('No data')]
    df5 = df5[~ df5['Foreign direct investment'].str.contains('x')]
    df5['Foreign direct investment'] = df5['Foreign direct investment'].str.replace(',', '.')
    df5[['Foreign direct investment']] = df5[['Foreign direct investment']].apply(pd.to_numeric)
    df5['Net_ODA_received_perc_of_GNI'] = df5['Net_ODA_received_perc_of_GNI'].str.replace(',', '.')
    df5[['Net_ODA_received_perc_of_GNI']] = df5[['Net_ODA_received_perc_of_GNI']].apply(pd.to_numeric)
    # Grouping the dataframe by 'Income Class' and calculating the mean of two metrics
    df5_groupby = df5.groupby(['Income classification according to WB'], as_index=False)
    df5 = df5_groupby[['Foreign direct investment', 'Net_ODA_received_perc_of_GNI']].mean()
    df5.rename(columns={'Income classification according to WB': 'Income Class',
                        'Foreign direct investment': 'Avg Foreign direct investment',
                        'Net_ODA_received_perc_of_GNI': 'Avg_Net_ODA_received_perc_of_GNI'}, inplace=True)
    df5.set_index('Income Class', inplace=True)
    #################################################

    log("QUESTION 5", output_df=df5, other=df5.shape)
    return df5


def question_6(df2):
    """
    :param df2: the dataframe created in question 2
    :return: lst
            Data Type: list
            Please read the assignment specs to know how to create the output dataframe
    """
    cities_lst = []
    #################################################
    df6 = df2.copy()
    # Filter 'LIC' countries from dataframe
    df6 = df6[df6['Income classification according to WB'] == 'LIC'][['Cities']]
    # Split 'Cities' column and drop NaN rows
    df6['Cities'] = df6['Cities'].apply(lambda x: [json.loads(s=a) for a in x.split('|||') if "null" not in a])
    df = pd.DataFrame(columns=pd.DataFrame(df6['Cities'][0]).columns)
    # Convert each row of 'Cities' to dataframe and append to df
    tmp = df6['Cities'].apply(lambda x: pd.DataFrame(x))
    df = df.append([i for i in tmp])
    # Sort dataframe by 'Population' and get top 5 most populous cities
    df = df.sort_values(by='Population', ascending=False)
    df = df[['City']]
    cities_lst = list(df.iloc[0:5, 0])
    lst = cities_lst
    #################################################

    log("QUESTION 6", output_df=None, other=cities_lst)
    return lst


def question_7(df2):
    """
    :param df2: the dataframe created in question 2
    :return: df7
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    df7 = df2.copy()
    df7['Cities'] = df7['Cities'].str.replace('\|\|\|', ',')
    df7 = df7[['Cities']].reset_index()
    df = pd.DataFrame(columns=['City', 'Country'])
    # Convert each row of 'Cities' to dataframe by using 'json.loads'
    tmp = df7['Cities'].apply(lambda x: pd.DataFrame(json.loads(s='[' + x + ']')))
    df7 = df.append([i[['Country', 'City']] for i in tmp])
    # Delete duplicate rows
    df7.drop_duplicates(subset=['City', 'Country'], keep='first', inplace=True)
    # Drop those city names only appearing in one country
    count = df7.groupby('City')['Country'].count().to_dict()
    df7['Count'] = df7['City'].map(count)
    df7 = df7[df7['Count'] > 1]
    df7 = df7.groupby(['City'])['Country'].apply(list).to_frame()
    #################################################

    log("QUESTION 7", output_df=df7, other=df7.shape)
    return df7


def question_8(df2, continents):
    """
    :param df2: the dataframe created in question 2
    :param continents: the path for the Countries-Continents.csv file
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    def process_8(x):
        a = '[' + x + ']'
        tmp_js = json.loads(s=a)
        tmp_df = pd.DataFrame(tmp_js)
        tmp_df = tmp_df.dropna(subset=['Population'], how='any')
        sum_population = tmp_df['Population'].sum()
        return sum_population

    df8 = df2.copy()
    df_continents = pd.read_csv(continents)
    df8 = df8[['Cities']]
    df8['Cities'] = df8['Cities'].str.replace('\|\|\|', ',')
    # Calculate the population of each country and add a new column 'Population'
    population_per_country = list(df8['Cities'].apply(process_8))
    df8.insert(len(df8.columns), 'Population', population_per_country)
    df8 = df8.drop('Cities', 1)
    world_population = sum(population_per_country)
    df_merge = pd.merge(df8, df_continents, on='Country')
    df_merge = df_merge[df_merge['Continent'] == 'South America']
    # Calculate the percentage of the world population is living in each South American country
    df_merge['Per_Population'] = (df_merge['Population'] / world_population) * 100
    labels = ['{0} - {1:1.2f}%'.format(i, j) for i, j in zip(df_merge["Country"], df_merge["Per_Population"])]
    data = df_merge['Population']
    data.name = ''
    # Draw pie chart
    a = data.plot(kind='pie', labels=labels,
                  fontsize=8,
                  figsize=(10, 10),
                  title='Percentage of the world population is living in South America'
                  )
    l = plt.legend(loc='upper right', ncol=2)
    #################################################

    plt.savefig("{}-Q11.png".format(studentid))


def question_9(df2):
    """
    :param df2: the dataframe created in question 2
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    df9 = df2.copy()
    # First, filtering all rows without value and replacing ',' with '.'
    df9 = df9[~ df9['Foreign direct investment'].str.contains('x')]
    df9 = df9[~ df9['Foreign direct investment, net inflows percent of GDP'].str.contains('x')]
    df9 = df9[~ df9['Covid_19_Economic_exposure_index_Ex_aid_and_FDI'].str.contains('x')]
    df9 = df9[~ df9['Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import'].str.contains('x')]
    df9['Foreign direct investment'] = df9['Foreign direct investment'].str.replace(',', '.')
    df9[['Foreign direct investment']] = df9[['Foreign direct investment']].apply(pd.to_numeric)
    df9['Foreign direct investment, net inflows percent of GDP'] = df9[
        'Foreign direct investment, net inflows percent of GDP'].str.replace(',', '.')
    df9[['Foreign direct investment, net inflows percent of GDP']] = df9[
        ['Foreign direct investment, net inflows percent of GDP']].apply(pd.to_numeric)
    df9['Covid_19_Economic_exposure_index_Ex_aid_and_FDI'] = df9[
        'Covid_19_Economic_exposure_index_Ex_aid_and_FDI'].str.replace(',', '.')
    df9[['Covid_19_Economic_exposure_index_Ex_aid_and_FDI']] = df9[
        ['Covid_19_Economic_exposure_index_Ex_aid_and_FDI']].apply(pd.to_numeric)
    df9['Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import'] = df9[
        'Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import'].str.replace(',', '.')
    df9[['Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import']] = df9[
        ['Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import']].apply(pd.to_numeric)
    # Grouping data by 'Income Class' and calculating the mean
    df9_groupby = df9.groupby(['Income classification according to WB'], as_index=False)
    df9 = df9_groupby[['Covid_19_Economic_exposure_index_Ex_aid_and_FDI',
                       'Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import',
                       'Foreign direct investment, net inflows percent of GDP', 'Foreign direct investment']].mean()
    df9.set_index('Income classification according to WB', inplace=True)
    df9 = df9.reindex(index=['HIC', 'MIC', 'LIC'])
    # Draw bar chart
    plot = df9.plot(kind='bar',
                    figsize=(10, 10),
                    rot=0,
                    title='Compare the high, middle, and low income level countries based on several metrics')
    l = plt.legend(loc=2)
    #################################################

    plt.savefig("{}-Q12.png".format(studentid))


def question_10(df2, continents):
    """
    :param df2: the dataframe created in question 2
    :return: nothing, but saves the figure on the disk
    :param continents: the path for the Countries-Continents.csv file
    """

    #################################################
    def process_sum(x):
        a = '[' + x + ']'
        tmp_js = json.loads(s=a)
        tmp_df = pd.DataFrame(tmp_js)
        tmp_df = tmp_df.dropna(subset=['Population'], how='any')
        sum_population = tmp_df['Population'].sum()
        return sum_population

    def process_10(x):
        compare_country_dist = {'Burkina': 'Burkina Faso', 'Congo': 'Republic of the Congo', 'Burma (Myanmar)': 'Burma',
                                'Congo, Democratic Republic of': 'Democratic Republic of the Congo',
                                'East Timor': 'Timor-Leste',
                                'Korea, North': 'North Korea', 'Korea, South': 'South Korea',
                                'Russian Federation': 'Russia',
                                'CZ': 'Czech Republic', 'US': 'United States'
                                }
        if x in compare_country_dist:
            return compare_country_dist[x]
        else:
            return x

    # First, renaming these countries have name issue by using the dictionary created in Q4
    df_continents = pd.read_csv(continents)
    # According to the dictionary, renaming these countries
    df_continents["Country"] = df_continents["Country"].apply(process_10)
    df10 = df2.copy()
    # First of all, calculating the population of each country
    df10['Cities'] = df10['Cities'].str.replace('\|\|\|', ',')
    # Calculate each country's population and add a new column 'Population'
    population_per_country = list(df10['Cities'].apply(process_sum))
    df10.insert(len(df10.columns), 'Population', population_per_country)
    df10 = df10.reset_index()
    df10 = df10[['Country', 'avg_latitude', 'avg_longitude', 'Population']]
    df_merge = pd.merge(df10, df_continents, on='Country')
    df_merge = df_merge.sort_values(by='Continent')
    # Pick color to each continent
    all_continents = list(set(df_merge['Continent']))
    colormap = ['green', 'yellow', 'orange', 'red', 'blue', 'purple']
    continent_color = pd.DataFrame({'Continent': all_continents, 'Colormap': colormap})
    df_merge_color = pd.merge(df_merge, continent_color, on='Continent')
    # Draw scatter chart
    plot = df_merge_color.plot(kind="scatter", figsize=(10, 10), x="avg_longitude", y="avg_latitude", alpha=1,
                               s=df_merge_color["Population"] / 100000, c=df_merge_color['Colormap'],
                               title='The distribution of countries with population scale')
    # Set the legend
    cmap = [plt.Line2D(range(1), range(1), color=i, marker='o', linestyle='',
                       markersize=10) for i in colormap]
    l = plt.legend(cmap, all_continents)
    #################################################

    plt.savefig("{}-Q13.png".format(studentid))


if __name__ == "__main__":
    time_start = time.time()
    df1 = question_1("exposure.csv", "Countries.csv")
    df2 = question_2(df1.copy(True))
    df3 = question_3(df2.copy(True))
    df4 = question_4(df2.copy(True), "Countries-Continents.csv")
    df5 = question_5(df2.copy(True))
    lst = question_6(df2.copy(True))
    df7 = question_7(df2.copy(True))
    question_8(df2.copy(True), "Countries-Continents.csv")
    question_9(df2.copy(True))
    question_10(df2.copy(True), "Countries-Continents.csv")
    time_end = time.time()
    print('time cost: ', time_end - time_start, 's')