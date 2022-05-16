import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
import numpy as np
import math
import re

df = pd.read_csv('routes.csv')
df.head()

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


def question_1(routes, suburbs):
    """
    :param routes: the path for the routes dataset
    :param suburbs: the path for the routes suburbs
    :return: df1
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    def get_start(service_direction_data):
        Sentence_list = []
        for sentence in service_direction_data.split(','):
            Sentence_list.append(sentence)
        Direction_before_clean = Sentence_list[0]
        Direction_list = []
        for direction in Direction_before_clean.split(' to '):
            Direction_list.append(direction)
        return Direction_list[0]

    def get_end(service_direction_data):
        Sentence_list = []
        for sentence in service_direction_data.split(','):
            Sentence_list.append(sentence)
        Direction_before_clean = Sentence_list[-1]
        Direction_list = []
        for direction in Direction_before_clean.split(' to '):
            Direction_list.append(direction)
        return Direction_list[-1]

    df['start'] = df['service_direction_name'].apply(get_start)
    df['end'] = df['service_direction_name'].apply(get_end)

    df_q1 = pd.DataFrame(df, columns=['start', 'end'])
    df_q1 = pd.merge(df, df_q1, left_index=True, right_index=True)
    del df_q1['start_y']
    del df_q1['end_y']
    df_q1.rename(columns={'start_x': 'start'}, inplace=True)
    df_q1.rename(columns={'end_x': 'end'}, inplace=True)
    df1 = df_q1
    #################################################

    log("QUESTION 1", output_df=df1[["service_direction_name", "start", "end"]], other=df1.shape)
    return df1


def question_2(df1):
    """
    :param df1: the dataframe created in question 1
    :return: dataframe df2
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    df_start_end = pd.concat([df1.start, df1.end], ignore_index=True)
    df_startEnd = pd.DataFrame(df_start_end, columns=['service_location'])
    Frequency = df_startEnd.groupby('service_location')['service_location'].count()
    df_q2 = pd.DataFrame(Frequency)
    df_q2.rename(columns={'service_location': 'frequency'}, inplace=True)
    df_q2.reset_index(level=0, inplace=True)
    result2 = df_q2.sort_values(by=['frequency'], ascending=False)
    index_result2 = result2.head()
    final_result2 = index_result2.reset_index(drop=True)
    df2 = final_result2
    #################################################

    log("QUESTION 2", output_df=df2, other=df2.shape)
    return df2


def question_3(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df3
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    def map_transport(transport_name):
        TransportName_list = []
        for transport in transport_name.split(' '):
            TransportName_list.append(transport)
        if 'buses' in TransportName_list or 'Bus' in TransportName_list or 'bus' in TransportName_list or 'Buses' in TransportName_list or 'buses' in TransportName_list or 'coach' in TransportName_list:
            transport_name = 'Bus'
        elif 'Ferry' in TransportName_list or 'ferry' in TransportName_list or 'Ferries' in TransportName_list or 'ferries' in TransportName_list:
            transport_name = 'Ferry'
        elif 'Light' in TransportName_list and 'Rail' in TransportName_list:
            transport_name = 'Light Rail'
        elif 'Train' in TransportName_list or 'train' in TransportName_list or 'Trains' in TransportName_list or 'trains' in TransportName_list:
            transport_name = 'Train'
        elif 'Metro' in TransportName_list or 'metro' in TransportName_list:
            transport_name = 'Metro'
        elif 'Network' in TransportName_list:
            if 'Train' in TransportName_list or 'train' in TransportName_list or 'Trains' in TransportName_list or 'trains' in TransportName_list:
                transport_name = 'Train'
            else:
                transport_name = 'Bus'
        return transport_name

    df1['transport_name'] = df1['transport_name'].apply(map_transport)
    df3 = df1
    #################################################

    log("QUESTION 3", output_df=df3[['transport_name']], other=df3.shape)
    return df3


def question_4(df3):
    """
    :param df3: the dataframe created in question 3
    :param continents: the path for the Countries-Continents.csv file
    :return: df4
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    df_transport = pd.DataFrame(df3['transport_name'])
    transport_frequency = df_transport.groupby('transport_name')['transport_name'].count()
    df_transport_frequency = pd.DataFrame(transport_frequency)
    df_transport_frequency.rename(columns={'transport_name': 'frequency'}, inplace=True)
    df_transport_frequency.reset_index(level=0, inplace=True)
    df_q4 = df_transport_frequency.sort_values(by='frequency')
    df4 = df_q4.reset_index(drop=True)
    #################################################

    log("QUESTION 4", output_df=df4[["transport_name", "frequency"]], other=df4.shape)
    return df4


def question_5(df3, suburbs):
    """
    :param df3: the dataframe created in question 2
    :param suburbs : the path to dataset
    :return: df5
            Data Type: dataframe
            Please read the assignment specs to know how to create the output dataframe
    """
    #################################################
    df_suburbs = pd.read_csv('suburbs.csv')
    df_ratio = pd.DataFrame(df3.depot_name, columns=['depot_name'])
    df_suburb_population = pd.DataFrame(df_suburbs, columns=['suburb', 'population'])
    df_depot_frequency = pd.DataFrame(df_ratio.groupby('depot_name')['depot_name'].count())
    df_depot_frequency.rename(columns={'depot_name': 'frequency'}, inplace=True)
    df_depot_frequency.reset_index(level=0, inplace=True)
    df5 = pd.merge(df_depot_frequency, df_suburb_population, left_on=['depot_name'], right_on=['suburb'])
    del df5['suburb']
    df5.rename(columns={'depot_name': 'depot'}, inplace=True)
    df5['ratio'] = df5['frequency'] / df5['population']
    df5 = df5[df5['ratio'] < 1]
    del df5['frequency']
    del df5['population']
    df5 = df5.sort_values(by=['ratio'], ascending=False)
    df5 = df5.set_index('depot')
    df5 = df5.head(5)
    #################################################

    log("QUESTION 5", output_df=df5[["ratio"]], other=df5.shape)
    return df5


def question_6(df3):
    """
    :param df3: the dataframe created in question 3
    :return: nothing, but saves the figure on the disk
    """
    table = None
    #################################################
    operator_transport = df3.groupby(['operator_name', 'transport_name'])['transport_name'].count()
    operator_transport
    df_operator = pd.DataFrame(operator_transport)
    df_operator.columns = ['all_transport_sum']
    df_operator.reset_index(inplace=True)
    df_group = df_operator.merge(right=df3[['operator_name', 'transport_name']], on=['operator_name', 'transport_name'],
                                 how='left')
    df_group.drop_duplicates(inplace=True)
    # df_group
    table = df_group.pivot_table(index='operator_name', columns='transport_name', values='all_transport_sum')
    table.fillna(0, inplace=True)
    #################################################

    log("QUESTION 6", output_df=None, other=table)
    return table


def question_7(df3, suburbs):
    """
    :param df3: the dataframe created in question 3
    :param suburbs : the path to dataset
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    df_suburbs = pd.read_csv('suburbs.csv')
    df_q7 = df_suburbs[df_suburbs['statistic_area'] == 'Greater Sydney']
    df_q7 = pd.DataFrame(df_q7, columns=['local_goverment_area', 'population', 'median_income', 'sqkm'])
    df_q7['per_capita_area'] = df_q7['sqkm'] / df_q7['population']
    df_q7

    plt.figure(figsize=(30, 300))
    plt.style.use('seaborn-talk')

    fig, axes = plt.subplots(nrows=4, ncols=1)
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, hspace=2.5)
    fig.suptitle('z5377912_q7.png')

    ax0 = df_q7.groupby('local_goverment_area')[['population']].sum().plot(kind='bar', ax=axes[0], color='Red',
                                                                           figsize=(30, 30))
    ax0.set_title('population_sum')
    ax1 = df_q7.groupby('local_goverment_area')[['median_income']].mean().plot(kind='bar', ax=axes[1], color='Green',
                                                                               figsize=(30, 30))
    ax1.set_title('median_income_mean')
    # plt.yscale('log')
    ax2 = df_q7.groupby('local_goverment_area')[['sqkm']].sum().plot(kind='bar', ax=axes[2], color='Blue',
                                                                     figsize=(30, 30), log = True)

    ax2.set_title('sqkm_sum')
    ax3 = df_q7.groupby('local_goverment_area')[['population']].sum().plot(kind='bar', ax=axes[3], figsize=(30, 30))
    ax3 = df_q7.groupby('local_goverment_area')[['median_income']].mean().plot(kind='bar', ax=axes[3], color='Green')
    ax3.set_title('population_median_income')
    #################################################

    plt.savefig("{}-Q7.png".format(studentid))


def question_8(df3):
    """
    :param df3: the dataframe created in question 3
    :param suburbs : the path to dataset
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    df_suburbs_start = pd.read_csv('suburbs.csv')
    df_suburbs_end = pd.read_csv('suburbs.csv')

    df_suburbs = pd.DataFrame(df_suburbs_start[df_suburbs_start['state'] == 'NSW'],
                              columns=['suburb', 'lat', 'lng', 'sqkm'])
    df_suburbs_start = pd.DataFrame(df_suburbs_start[df_suburbs_start['state'] == 'NSW'],
                                    columns=['suburb', 'lat', 'lng'])
    df_suburbs_start.rename(columns={'suburb': 'suburb_start'}, inplace=True)
    df_suburbs_end = pd.DataFrame(df_suburbs_end[df_suburbs_end['state'] == 'NSW'], columns=['suburb', 'lat', 'lng'])
    df_suburbs_end.rename(columns={'suburb': 'suburb_end'}, inplace=True)

    df_useful_startEnd = df3[df3['start'].isin(df_suburbs['suburb']) & df3['end'].isin(df_suburbs['suburb'])]
    df_useful_startEnd = df_useful_startEnd[['start', 'end', 'transport_name']]

    df_data8 = pd.merge(df_useful_startEnd, df_suburbs_start, left_on=['start'], right_on=['suburb_start'])
    df_data8 = df_data8.merge(right=df_suburbs_end, left_on='end', right_on='suburb_end')
    df_data8 = df_data8[df_data8['transport_name'] != 'Bus']

    plt.figure(figsize= (20,20))
    plt.hist2d(df_suburbs['lng'], df_suburbs['lat'], bins=350, cmap='Greens', weights=df_suburbs['sqkm'])
    plt.clim(1, 2)
    df_q8 = df_data8.groupby(by='transport_name')
    # colour = {'Ferry':'r', 'Train':'g', 'Metro':'b'}
    for transport_name, data in df_q8:
        if transport_name == 'Train':
            plt.plot((data['lng_x'], data['lng_y']), (data['lat_x'], data['lat_y']), c='r', linewidth=1)
        elif transport_name == 'Ferry':
            plt.plot((data['lng_x'], data['lng_y']), (data['lat_x'], data['lat_y']), c='b', linewidth=1)
        elif transport_name == 'Metro':
            plt.plot((data['lng_x'], data['lng_y']), (data['lat_x'], data['lat_y']), c='g', linewidth=1)
    plt.xlabel('longtitude')
    plt.ylabel('latitude')
    plt.scatter([], [], color='red', s=100, label='Train')
    plt.scatter([], [], color='blue', s=100, label='Ferry')
    plt.legend()
    plt.title('z5377912_q7.png')
    #################################################

    plt.savefig("{}-Q8.png".format(studentid))



if __name__ == "__main__":
    df1 = question_1("routes.csv", "suburbs.csv")
    df2 = question_2(df1.copy(True))
    df3 = question_3(df1.copy(True))
    df4 = question_4(df3.copy(True))
    df5 = question_5(df3.copy(True), "suburbs.csv")
    table = question_6(df3.copy(True))
    question_7(df3.copy(True), "suburbs.csv")
    question_8(df3.copy(True))