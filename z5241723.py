from flask import Flask, request, Response
from flask_restx import Resource, Api
from flask_restx import fields
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import sqlite3
import requests
import json
import time
import math
import os

app = Flask(__name__)
api = Api(app, default="TV Show", title="TV Show database", description="API for TV Show.")

resource_fields = api.model('Resource', {
    'id': fields.Integer,
    'tvmaze_id': fields.Integer,
    'name': fields.String,
    'type': fields.String,
    'language': fields.String,
    'genres': fields.List(fields.String),
    'status': fields.String,
    'runtime': fields.Integer,
    'premiered': fields.String,
    'officialSite': fields.String,
    'schedule_time': fields.String,
    'schedule_days': fields.List(fields.String),
    'last_update': fields.String,
    'rating_average': fields.Float,
    'weight': fields.Integer,
    'summary': fields.String,
    'self_href': fields.String,
    'previous_href': fields.String,
    'next_href': fields.String
})

parser = api.parser()
parser.add_argument('name', type=str)

parser_1 = api.parser()
parser_1.add_argument('order_by', type=str, default='+id')
parser_1.add_argument('page', type=int, default=1)
parser_1.add_argument('page_size', type=int, default=100)
parser_1.add_argument('filter', type=str, default='id,name')

parser_2 = api.parser()
parser_2.add_argument('format', type=str)
parser_2.add_argument('by', type=str)


def operate_database(db, operation):
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    cursor.execute(operation)
    res = cursor.fetchall()
    connection.commit()
    connection.close()
    return res


@api.route('/tv-shows/import')
@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.response(409, 'Confliction')
@api.response(201, 'Created')
@api.doc(parser=parser)
class question1(Resource):
    @api.doc(description="Import a TV show")
    def post(self):
        name = parser.parse_args()['name']
        # The input name cannot be None
        if name is None:
            return {'message': 'Name cannot be none'}, 404
        # Request TV show from tvmaze api
        url = "http://api.tvmaze.com/search/shows?q=" + name
        r = requests.get(url)
        data = r.json()
        # The result is null
        if not data:
            return {'message': "TV show {} doesn't exist in the data source".format(name)}, 404
        # Only keep English alpha and number
        tmp_name = "".join(filter(str.isalnum, name.lower()))
        # Set id of this tv show in database
        res = operate_database('z5241723.db', 'SELECT max(id) FROM Shows')
        current_id = res[0][0]
        if current_id is None:
            new_id = 1
        else:
            new_id = int(current_id) + 1
        for i in data:
            # Exact match
            # Only keep English alpha and number
            if "".join(filter(str.isalnum, ((i['show']['name']).lower()))) == tmp_name:
                # If this tv show has already existed in database
                if operate_database('z5241723.db', 'SELECT id FROM Shows WHERE tvmaze_id = {}'.format(i['show']['id'])):
                    return {'message': "TV show {} has already existed in the data source".format(name)}, 409
                d = i['show']
                self_href = "http://127.0.0.1:5000/tv-shows/" + str(new_id)
                # Create dictionary to store information needed to insert in database
                the_dict = {'id': new_id, 'tvmaze_id': d['id'], 'name': d['name'], 'type': d['type'],
                            'language': d['language'], 'genres': ','.join(d['genres']), 'status': d['status'],
                            'runtime': d['runtime'], 'premiered': d['premiered'], 'officialSite': d['officialSite'],
                            'schedule_time': 'NULL', 'schedule_days': 'NULL',
                            'last_updated': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                            'rating_average': 'NULL', 'weight': d['weight'], 'network_id': 'NULL',
                            'network_name': 'NULL', 'country_name': 'NULL',
                            'country_code': 'NULL', 'country_timezone': 'NULL',
                            'summary': d['summary'].replace('\'', '\\'), 'self_href': self_href}

                for j in the_dict.keys():
                    if the_dict[j] is None:
                        the_dict[j] = 'NULL'

                if d['schedule'] is not None:
                    for k in d['schedule'].keys():
                        if d['schedule'][k] is not None:
                            the_dict['schedule_' + k] = d['schedule'][k]

                if d['rating'] is not None:
                    for k in d['rating'].keys():
                        if d['rating'][k] is not None:
                            the_dict['rating_' + k] = d['rating'][k]

                if d['network'] is not None:
                    for k in d['network'].keys():
                        if k != 'country' and d['network'][k] is not None:
                            the_dict['network_' + k] = d['network'][k]
                        elif k == 'country' and d['network'][k] is not None:
                            for key in d['network']['country'].keys():
                                if d['network']['country'][key] is not None:
                                    the_dict['country_' + key] = d['network']['country'][key]
                # Insert into table Shows
                operation = "INSERT INTO Shows VALUES ({}, {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, {}, {}, '{}', '{}', '{}', '{}', '{}', '{}');" \
                    .format(the_dict['id'], the_dict['tvmaze_id'], the_dict['name'], the_dict['type'], the_dict['language'], the_dict['genres'],
                            the_dict['status'], the_dict['runtime'], the_dict['premiered'], the_dict['officialSite'],
                            the_dict['schedule_time'], ','.join(the_dict['schedule_days']), the_dict['last_updated'],
                            the_dict['rating_average'], the_dict['weight'], the_dict['network_id'], the_dict['network_name'],
                            the_dict['country_name'], the_dict['country_code'], the_dict['country_timezone'],
                            the_dict['summary'], the_dict['self_href'])
                r = operate_database('z5241723.db', operation)
                information = {
                    "id": new_id,
                    "last-update": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    "tvmaze-id": d['id'],
                    "_links": {
                        "self": {
                            "href": self_href
                        }
                    }
                }
                r_pre = operate_database('z5241723.db', 'SELECT max(id) FROM Shows WHERE id < {}'.format(new_id))
                if r_pre[0][0] is not None:
                    information["_links"]["previous"] = {"href": "http://127.0.0.1:5000/tv-shows/" + str(r_pre[0][0])}
                return information, 201
        return {'message': "TV show {} doesn't exist in the data source".format(name)}, 404


@api.route('/tv-shows/<int:id>')
@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
class question234(Resource):
    @api.doc(description="Get a TV show")
    def get(self, id):
        shows_operation = "SELECT * FROM Shows WHERE id = {}".format(id)
        shows_r = operate_database('z5241723.db', shows_operation)
        # The result id null
        if not shows_r:
            return {'message': "The tv show with id {} doesn't exist in the database.".format(id)}, 404
        # Find the previous and next shows of this show
        r_pre = operate_database('z5241723.db', 'SELECT max(id) FROM Shows WHERE id < {}'.format(id))
        if r_pre[0][0] is not None:
            pre_href = "http://127.0.0.1:5000/tv-shows/" + str(r_pre[0][0])
        else:
            pre_href = 'NULL'
        r_next = operate_database('z5241723.db', 'SELECT min(id) FROM Shows WHERE id > {}'.format(id))
        if r_next[0][0] is not None:
            next_href = "http://127.0.0.1:5000/tv-shows/" + str(r_next[0][0])
        else:
            next_href = 'NULL'
        # The information needed to return
        information = {
            "tvmaze_id": shows_r[0][1],
            "id": shows_r[0][0],
            "last-update": shows_r[0][12],
            "name": shows_r[0][2],
            "type": shows_r[0][3],
            "language": shows_r[0][4],
            "genres": [i for i in shows_r[0][5].split(',')],
            "status": shows_r[0][6],
            "runtime": shows_r[0][7],
            "premiered": shows_r[0][8],
            "officialSite": shows_r[0][9],
            "schedule": {
                "time": shows_r[0][10],
                "days": [i for i in shows_r[0][11].split(',')]
            },
            "rating": {
                "average": shows_r[0][13]
            },
            "weight": shows_r[0][14],
            "network": {
                "id": shows_r[0][15],
                "name": shows_r[0][16],
                "country": {
                    "name": shows_r[0][17],
                    "code": shows_r[0][18],
                    "timezone": shows_r[0][19]
                }
            },
            "summary": shows_r[0][20].replace('\\', '\''),
            "_links": {
                "self": {
                    "href": shows_r[0][21]
                }
            }
        }
        if pre_href != 'NULL':
            information["_links"]["previous"] = {"href": pre_href}
        if next_href != 'NULL':
            information["_links"]["next"] = {"href": next_href}
        return information, 200

    @api.doc(description="Delete a TV show")
    def delete(self, id):
        shows_operation = "SELECT * FROM Shows WHERE id = {}".format(id)
        shows_r = operate_database('z5241723.db', shows_operation)
        # The result id null
        if not shows_r:
            return {'message': "The tv show with id {} doesn't exist in the database.".format(id)}, 404
        # Delete this tv show from database
        operate_database('z5241723.db', "DELETE FROM Shows WHERE id = {}".format(id))
        information = {
            "message": "The tv show with id {} was removed from the database!".format(id),
            "id": id
        }
        return information, 200

    @api.doc(description="Update a TV show")
    @api.expect(resource_fields)
    def patch(self, id):
        shows_operation = "SELECT * FROM Shows WHERE id = {}".format(id)
        shows_r = operate_database('z5241723.db', shows_operation)
        # The result id null
        if not shows_r:
            return {'message': "The tv show with id {} doesn't exist in the database.".format(id)}, 404

        the_dict = {'id': shows_r[0][0], 'tvmaze_id': shows_r[0][1], 'name': shows_r[0][2], 'type': shows_r[0][3],
                    'language': shows_r[0][4], 'genres': shows_r[0][5], 'status': shows_r[0][6],
                    'runtime': shows_r[0][7], 'premiered': shows_r[0][8], 'officialSite': shows_r[0][9],
                    'schedule_time': shows_r[0][10], 'schedule_days': shows_r[0][11], 'last_updated': shows_r[0][12],
                    'rating_average': shows_r[0][13], 'weight': shows_r[0][14], 'network_id': shows_r[0][15],
                    'network_name': shows_r[0][16], 'country_name': shows_r[0][17], 'country_code': shows_r[0][18],
                    'country_timezone': shows_r[0][19], 'summary': shows_r[0][20], 'self_href': shows_r[0][21]}
        # Get the api_model
        tv_show = request.json
        if 'id' in tv_show and id != tv_show['id']:
            return {"message": "Id cannot be changed!"}, 400
        if 'tvmaze_id' in tv_show and the_dict['tvmaze_id'] != tv_show['tvmaze_id']:
            return {"message": "tvmaze_id cannot be changed!"}, 400
        if 'last_update' in tv_show:
            return {"message": "last_update cannot be changed!"}, 400
        if 'self_href' in tv_show and the_dict['self_href'] != tv_show['self_href']:
            return {"message": "self_href cannot be changed!"}, 400
        if 'previous_href' in tv_show:
            return {"message": "previous_href cannot be changed!"}, 400
        if 'next_href' in tv_show:
            return {"message": "next_href cannot be changed!"}, 400
        # Update the information of this tv show
        for key in tv_show:
            if key not in resource_fields.keys():
                return {"message": "Property {} is invalid".format(key)}, 400
            if key == 'genres' or key == 'schedule_days':
                tv_show[key] = ','.join(tv_show[key])
            if key == 'summary':
                tv_show[key] = tv_show[key].replace('\'', '\\')
            if key != 'id':
                if isinstance(tv_show[key], int) or isinstance(tv_show[key], float):
                    operate_database('z5241723.db', "UPDATE Shows set {} = {} WHERE id={}".format(key, tv_show[key], id))
                else:
                    operate_database('z5241723.db', "UPDATE Shows set {} = '{}' WHERE id={}".format(key, tv_show[key], id))
        # Change the last update time
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        operate_database('z5241723.db', "UPDATE Shows set '{}' = '{}' WHERE id={}".format('last_update', current_time, id))
        information = {
            "id": id,
            "last-update": current_time,
            "_links": {
                "self": {
                    "href": the_dict['self_href']
                }
            }
        }
        # Find the previous and next shows of this show
        r_pre = operate_database('z5241723.db', 'SELECT max(id) FROM Shows WHERE id < {}'.format(id))
        if r_pre[0][0] is not None:
            pre_href = "http://127.0.0.1:5000/tv-shows/" + str(r_pre[0][0])
        else:
            pre_href = 'NULL'
        r_next = operate_database('z5241723.db', 'SELECT min(id) FROM Shows WHERE id > {}'.format(id))
        if r_next[0][0] is not None:
            next_href = "http://127.0.0.1:5000/tv-shows/" + str(r_next[0][0])
        else:
            next_href = 'NULL'
        if pre_href != 'NULL':
            information["_links"]["previous"] = {"href": pre_href}
        if next_href != 'NULL':
            information["_links"]["next"] = {"href": next_href}
        return information, 200


@api.route('/tv-shows')
@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.doc(parser=parser_1)
class question5(Resource):
    @api.doc(description="Get all TV show")
    def get(self):
        # Get the query parameters
        order = parser_1.parse_args()['order_by']
        page = parser_1.parse_args()['page']
        page_size = parser_1.parse_args()['page_size']
        filters = parser_1.parse_args()['filter']
        if order is None or page is None or page_size is None or filters is None:
            return {'message': 'All four parameter should have value'}, 404
        if page == 0 or page_size == 0:
            return {'message': 'Page or Page_size cannot be 0'}, 404
        num_of_all_tv = operate_database('z5241723.db', 'SELECT count(*) FROM Shows')
        # The database is empty
        if num_of_all_tv[0][0] == 0:
            return {'message': 'The database is empty'}, 404
        # This page is empty
        max_page = math.ceil(num_of_all_tv[0][0] / page_size)
        if page > max_page:
            return {'message': "The page {} is empty".format(page)}, 404
        # The order and filter parameters must in below lists
        given_orders = ['id', 'name', 'runtime', 'premiered', 'rating_average']
        given_filters = ['tvmaze_id', 'id', 'last_update', 'name', 'type', 'language', 'genres', 'status', 'runtime',
                         'premiered', 'officialSite', 'schedule', 'rating', 'weight', 'network', 'summary']
        # Format parameters to sql query
        filter_all = []
        for i in filters.split(','):
            i = i.replace('-', '_')
            if i not in given_filters:
                return {'message': 'TV shows cannot filter by {}'.format(i)}, 400
            if i == 'schedule':
                filter_all.extend(['schedule_time', 'schedule_days'])
            elif i == 'rating':
                filter_all.append('rating_average')
            elif i == 'network':
                filter_all.extend(['network_id', 'network_name', 'country_name', 'country_code', 'country_timezone'])
            else:
                filter_all.append(i)
        if len(set(filter_all)) != len(filter_all):
            return {'message': 'The filter cannot have duplicates'}, 400
        order_by = ''
        for i in order.split(','):
            tmp_order = i[1:].replace('-', '_')
            if tmp_order not in given_orders:
                return {'message': 'TV shows cannot order by {}'.format(tmp_order)}, 400
            if i[0] == '+':
                order_by = order_by + tmp_order + ' ASC, '
            elif i[0] == '-':
                order_by = order_by + tmp_order + ' DESC, '
            else:
                return {'message': 'The order_by must indicate ACS or DESC (i.e. +/-)'}, 400
        order_by = order_by[:-2]
        p1 = ','.join(filter_all)
        p2 = order_by
        r = operate_database('z5241723.db', "SELECT {} FROM Shows ORDER BY {}".format(p1, p2))
        pre_page = 0
        next_page = 0
        # Calculate which tv shows will be present in this page
        if page == 1:
            if num_of_all_tv[0][0] > page_size:
                r = r[0:page_size]
                next_page = 2
            else:
                r = r[0:num_of_all_tv[0][0]]
        else:
            if page * page_size >= num_of_all_tv[0][0]:
                r = r[(page - 1) * page_size:num_of_all_tv[0][0]]
                pre_page = page - 1
            else:
                r = r[(page - 1) * page_size:page * page_size]
                pre_page = page - 1
                next_page = page + 1
        # The information used to return
        information = {
            "page": page,
            "page-size": page_size,
            "tv-shows": [],
            "_links": {
                "self": {
                    "href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(order, page, page_size, filters)
                }
            }
        }
        if pre_page != 0:
            information['_links']['previous'] = \
                {"href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(order, pre_page, page_size, filters)}
        if next_page != 0:
            information['_links']['next'] = \
                {"href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(order, next_page, page_size, filters)}
        # Add structure to information['tv-shows']
        result_of_index = dict()
        index = 0
        tvs = dict()
        for i in filter_all:
            if 'schedule' in i:
                tvs['schedule'] = {'time': 'NULL', 'days': 'NULL'}
            elif 'rating' in i:
                tvs['rating'] = {'average': 'NULL'}
            elif 'network' in i:
                tvs['network'] = {'id': 'NULL',
                                  'name': 'NULL',
                                  'country': {
                                      'name': 'NULL',
                                      'code': 'NULL',
                                      'timezone': 'NULL'
                                  }}
            elif 'country' in i:
                tvs['network'] = {'id': 'NULL',
                                  'name': 'NULL',
                                  'country': {
                                      'name': 'NULL',
                                      'code': 'NULL',
                                      'timezone': 'NULL'
                                  }}
            else:
                tvs[i] = 'NULL'
            result_of_index[i] = index
            index += 1
        # Add content to information['tv-shows']
        for i in r:
            tmp_tvs = tvs.copy()
            if 'schedule' in filters:
                tmp_tvs['schedule'] = tvs['schedule'].copy()
            if 'rating' in filters:
                tmp_tvs['rating'] = tvs['rating'].copy()
            if 'network' in filters:
                tmp_tvs['network'] = tvs['network'].copy()
                tmp_tvs['network']['country'] = tvs['network']['country'].copy()
            for j in filter_all:
                if j == 'genres':
                    tmp_tvs[j] = [k for k in i[result_of_index[j]].split(',')]
                elif j == 'rating_average':
                    tmp_tvs['rating']['average'] = i[result_of_index[j]]
                elif j == 'schedule_time':
                    tmp_tvs['schedule']['time'] = i[result_of_index[j]]
                elif j == 'schedule_days':
                    tmp_tvs['schedule']['days'] = [k for k in i[result_of_index[j]].split(',')]
                elif j == 'network_id':
                    tmp_tvs['network']['id'] = i[result_of_index[j]]
                elif j == 'network_name':
                    tmp_tvs['network']['name'] = i[result_of_index[j]]
                elif j == 'country_name':
                    tmp_tvs['network']['country']['name'] = i[result_of_index[j]]
                elif j == 'country_code':
                    tmp_tvs['network']['country']['code'] = i[result_of_index[j]]
                elif j == 'country_timezone':
                    tmp_tvs['network']['country']['timezone'] = i[result_of_index[j]]
                else:
                    tmp_tvs[j] = i[result_of_index[j]]
            information['tv-shows'].append(tmp_tvs)
        return information, 200


@api.route('/tv-shows/statistics')
@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.doc(parser=parser_2)
class question6(Resource):
    @api.doc(description="Get the statistics of the existing TV Show")
    def get(self):
        # Get query parameter
        need_format = parser_2.parse_args()['format']
        need_by = parser_2.parse_args()['by']
        if need_format is None or need_by is None:
            return {'message': 'These two parameter must have value'}, 404
        if need_format not in {'json', 'image'}:
            return {'message': 'The format parameter must be json or image'}, 400
        if need_by not in ['language', 'genres', 'status', 'type']:
            return {'message': 'The by parameter must be language, genres, status or type'}, 400
        # Calculate total number of tv shows
        t = operate_database('z5241723.db', 'SELECT count(*) FROM Shows')
        total = t[0][0]
        # Calculate total number of tv shows which were updated last 24 hours
        t_u = operate_database('z5241723.db', "SELECT count(*) FROM Shows WHERE last_update >= '{}'".format(
            (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')))
        total_updated = t_u[0][0]
        bar_data = dict()
        if need_by == 'genres':
            r = operate_database('z5241723.db', 'SELECT {}, count(*) FROM Shows GROUP BY {}'.format(need_by, need_by))
            res = operate_database('z5241723.db', 'SELECT genres FROM Shows')
            all_genres = []
            for i in res:
                all_genres.extend([k for k in i[0].split(',')])
                all_genres = list(set(all_genres))
            all_genres = list(set(all_genres))
            for i in all_genres:
                if i == '':
                    each_res = operate_database('z5241723.db',
                                                "SELECT count(*) FROM Shows WHERE genres LIKE '{}'".format(i))
                else:
                    each_res = operate_database('z5241723.db', "SELECT count(*) FROM Shows WHERE genres LIKE '%{}%'".format(i))
                if i not in bar_data.keys():
                    bar_data[i] = each_res[0][0]
                else:
                    bar_data[i] = bar_data[i] + each_res[0][0]
        else:
            r = operate_database('z5241723.db', 'SELECT {}, count(*) FROM Shows GROUP BY {}'.format(need_by, need_by))

        information = {
            'total': total,
            'total-updated': total_updated
        }
        if need_format == 'json':
            information['value'] = dict()
            for i in r:
                if i[0] == '':
                    tmp = "No " + need_by
                    information['value'][tmp] = "{:.1f}".format((i[1] / total) * 100)
                else:
                    information['value'][i[0]] = "{:.1f}".format((i[1] / total) * 100)
            return information, 200
        else:
            if os.path.exists('{}.png'.format(need_by)):
                os.remove('{}.png'.format(need_by))
            if need_by == 'genres':
                label = []
                data = []
                for i in r:
                    if i[0] == '':
                        tmp = "No " + need_by
                        label.append(tmp)
                    else:
                        label.append(i[0])
                    data.append((i[1] / total) * 100)
                plt.figure(figsize=(20, 20), dpi=100)
                plt.subplot(2, 1, 1)
                x, label, per = plt.pie(data, labels=label, autopct="%1.2f%%")
                label = [i.set_size(15) for i in label]
                per = [i.set_size(15) for i in per]
                plt.axis('equal')
                plt.legend(prop={'size': 15})
                plt.title("TV shows break down by {}, total={}, total-updated={}".format(need_by, total, total_updated), fontsize=18)
                plt.subplot(2, 1, 2)
                plt.bar(x=[i if i != "" else "No genres" for i in bar_data.keys()], height=bar_data.values())
                plt.xticks(rotation=60, fontsize=15)
                plt.yticks(fontsize=14)
                plt.title("Genres statistics for TV shows", fontsize=18)
                plt.savefig('{}.png'.format(need_by))
                with open('{}.png'.format(need_by), 'rb') as f:
                    image = f.read()
                return Response(image, mimetype='image/png')
            else:
                label = []
                data = []
                for i in r:
                    if i[0] == '':
                        tmp = "No " + need_by
                        label.append(tmp)
                    else:
                        label.append(i[0])
                    data.append((i[1] / total) * 100)
                plt.figure(figsize=(20, 10), dpi=100)
                x, label, per = plt.pie(data, labels=label, autopct="%1.2f%%")
                label = [i.set_size(15) for i in label]
                per = [i.set_size(15) for i in per]
                plt.axis('equal')
                plt.legend(prop={'size': 15})
                plt.title("TV shows break down by {}, total={}, total-updated={}".format(need_by, total, total_updated), fontsize=18)
                plt.savefig('{}.png'.format(need_by))
                with open('{}.png'.format(need_by), 'rb') as f:
                    image = f.read()
                return Response(image, mimetype='image/png')



if __name__ == '__main__':
    r1 = operate_database('z5241723.db',
                          '''CREATE TABLE IF NOT EXISTS Shows(
                        id INTEGER PRIMARY KEY NOT NULL,
                        tvmaze_id INTEGER,
                        name TEXT,
                        type TEXT,
                        language TEXT,
                        genres TEXT,
                        status TEXT,
                        runtime INTEGER,
                        premiered TEXT,
                        officialSite TEXT,
                        schedule_time TEXT,
                        schedule_days TEXT,
                        last_update TEXT,
                        rating_average REAL,
                        weight INTEGER,
                        network_id INTEGER,
                        network_name TEXT,
                        country_name TEXT,
                        country_code TEXT,
                        country_timezone TEXT,
                        summary TEXT,
                        self_href TEXT
                        )'''
                          )
    app.run(host='127.0.0.1', port=5000, debug=True)
