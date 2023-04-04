from calendar import month
from flask import Flask, render_template, request
import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from dotenv import load_dotenv
from openpyxl import load_workbook
import sqlite3
import utils
from nltk.tokenize import word_tokenize
import nltk
import en_core_web_sm
import locationtagger
from statistics import mean
import geopy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from textblob import TextBlob
from nltk.corpus import words
nltk.download('maxent_ne_chunker')
  
nltk.download('words')
correct_words = words.words()
  
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
load_dotenv()



app = Flask(__name__)

filename=''
filename_for_database=''
products=set

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload-dataset', methods=['GET','POST'])
def main():
    f = request.files['file'] #File input
    structured=request.form.get('structure')
    if not f:
        return "No file attached"

    global filename
    filename=f.filename #changing global value of filename

    path='{}/{}'.format('static',filename)
    f.save(path)
    x=path.split('.')[-1]
    global filename_for_database
    filename_for_database='db/'+filename.replace(x,'db') #changing global value of filename_for_database

    #reading filedata start
    if x=='xlsx':
        new_wb = load_workbook(path)
        Dataframe = pd.read_excel(new_wb,engine='openpyxl',index_col=0,)
    elif x=='csv':
        Dataframe = pd.read_csv(path, encoding = "ISO-8859-1")
    else:
        return('Please upload the file in xlsx or csv only')
    #reading filedata end

    list_of_columns=list(Dataframe.columns) #getting columns from dataset
    
    #checking for data for insights
    data_report={'review':False,'order-status':False}

    # Data preprocessing and identifying columns in dataset START
    if 'product name' not in list_of_columns and 'Product Name' not in list_of_columns and 'name' not in list_of_columns and 'Name' not in list_of_columns:
        product_name=utils.check_product_name(list_of_columns=list_of_columns)
        Dataframe.rename(columns={product_name:'product_name'}, inplace=True)
    else:
        possible_values=['product name' , 'Product Name' ,'name', 'Name']
        for values in possible_values:
            Dataframe.rename(columns={values:'product_name'}, inplace=True)
    if 'review text' not in list_of_columns and 'Reviews' not in list_of_columns and 'Review Text' not in list_of_columns and 'reviews' not in list_of_columns and 'review.text' not in list_of_columns:
        review_text=utils.check_review_text(list_of_columns=list_of_columns)
        if review_text:
            Dataframe.rename(columns={review_text:'review_text'}, inplace=True)
            data_report['review']=True
    else:
        possible_values=['review text','Reviews','Review Text','reviews','review.text']
        for values in possible_values:
            Dataframe.rename(columns={values:'review_text'}, inplace=True)
        data_report['review']=True
    if 'product image' not in list_of_columns and 'Product Image' not in list_of_columns and 'image' not in list_of_columns and 'Image' not in list_of_columns:
        product_image=utils.check_product_image(list_of_columns=list_of_columns)
        Dataframe.rename(columns={product_image:'product_image'}, inplace=True)
    else:
        possible_values=['product image' , 'Product Image', 'image' , 'Image']
        for values in possible_values:
            Dataframe.rename(columns={values:'product_image'}, inplace=True)
    if 'Order Country' not in list_of_columns and 'order country' not in list_of_columns:
        product_country=utils.check_product_country(list_of_columns=list_of_columns)
        print(product_country)
        Dataframe.rename(columns={product_country:'order_country'}, inplace=True)
    else:
        possible_values=['Order Country', 'order country' ]
        for values in possible_values:
            print(values)
            Dataframe.rename(columns={values:'order_country'}, inplace=True)        
    if 'Order City' not in list_of_columns and 'order city' not in list_of_columns:
        product_city=utils.check_product_city(list_of_columns=list_of_columns)
        Dataframe.rename(columns={product_city:'order_city'}, inplace=True)
    else:
        possible_values=['Order City' , 'order city' ]
        for values in possible_values:
            Dataframe.rename(columns={values:'order_city'}, inplace=True)  
    if 'Order State' not in list_of_columns and 'order state' not in list_of_columns:
        product_state=utils.check_product_state(list_of_columns=list_of_columns)
        Dataframe.rename(columns={product_state:'order_state'}, inplace=True)
    else:
        possible_values=['Order State' , 'order state' ]
        for values in possible_values:
            Dataframe.rename(columns={values:'order_state'}, inplace=True)  
    if 'review text' not in list_of_columns and 'Reviews' not in list_of_columns and 'Review Text' not in list_of_columns and 'reviews' not in list_of_columns and 'review.text' not in list_of_columns:
        review_text=utils.check_review_text(list_of_columns=list_of_columns)
        if review_text:
            Dataframe.rename(columns={review_text:'review_text'}, inplace=True)
            data_report['review']=True
    else:
        possible_values=['review text','Reviews','Review Text','reviews','review.text']
        for values in possible_values:
            Dataframe.rename(columns={values:'review_text'}, inplace=True)
        data_report['review']=True
    if 'shipping.date' not in list_of_columns and 'shipping date' not in list_of_columns and 'Shipping Date' not in list_of_columns:
        shipping_date=utils.check_shipping_date(list_of_columns=list_of_columns)
        if shipping_date:
            Dataframe.rename(columns={shipping_date:'shipping_date'}, inplace=True)
    else:
        possible_values=['shipping.date', 'shipping date', 'Shipping Date']
        for values in possible_values:
            Dataframe.rename(columns={values:'shipping_date'}, inplace=True)
    if 'Order Status' not in list_of_columns and 'order.status' not in list_of_columns and 'order status' not in list_of_columns:
        order_status=utils.check_order_status(list_of_columns=list_of_columns)
        if order_status:
            Dataframe.rename(columns={shipping_date:'order_status'}, inplace=True)
            data_report['order-status']=True
    else:
        possible_values=['Order Status','order.status','order status']
        for values in possible_values:
            Dataframe.rename(columns={values:'order_status'}, inplace=True)
        data_report['order-status']=True
    # Identifying columns in dataset END
    con=sqlite3.connect(filename_for_database) #connecting to the database
    cursor=con.cursor()

    for column in list_of_columns:
        final_name=column.lower().replace(" ","_")
        final_name=final_name.lower().replace("(","_")
        final_name=final_name.lower().replace(")","_")
        Dataframe.rename(columns={column:final_name}, inplace=True)
    #Data preprocessing END

    # Uncomment to enter a new dataset
    Dataframe.to_sql(name='Dataset',con=con,if_exists='replace') # Dataset converted to RDBMS table

    con.commit()
    
    cursor.execute('select "product_name" from Dataset;')
    result = cursor.fetchall()
    global products
    products=set(product[0] for product in result) # List of products fetched to be listed on page
    if structured=='structured':
        return render_template('check.html', products=list(products), final_report=data_report, db=filename_for_database, structured=True)
    else:
        return render_template('check.html', products=list(products), final_report=data_report, db=filename_for_database)

@app.route('/get-insights', methods=['GET','POST'])
def insights():
    #called for insights of products
    db=request.form.get("db")
    global filename_for_database
    product=str(request.form.get('products'))
    products=(product,)
    Reviews=request.form.get('review')
    order_status=request.form.get('order_status')
    #For unstructured dataset Reviews are listed based on the result of sentiment analysis
    if Reviews=='True':
        filename_for_database=db
        con=sqlite3.connect(filename_for_database) #connecting to the database
        cursor=con.cursor()
        cursor.execute('SELECT review_text FROM Dataset WHERE product_name = ?',products)
        data=cursor.fetchall()
        nltk.download('vader_lexicon')
        reviews={'Negative':0,'Positive':0}
        for review in data:
            sid=SentimentIntensityAnalyzer()
            sa=sid.polarity_scores(str(review))
            if sa['neg']>sa['pos']:
                reviews['Negative']=reviews['Negative']+1
            else:
                reviews['Positive']=reviews['Positive']+1
        return { 'review':reviews }

    #For structured dataset OrderStatuses are listed:
    if order_status=='True':
        filename_for_database=db
        con=sqlite3.connect(filename_for_database) #connecting to the database
        cursor=con.cursor()
        cursor.execute('SELECT product_name, product_image, order_status FROM Dataset WHERE product_name = ?',products)
        data=cursor.fetchall()
        img=data[0][1]
        status={}
        for i in data:
            if i[2] in status.keys():
                status[i[2]]=status[i[2]]+1
            else:
                status[i[2]]=1
    return {'img':img, 'status':status}

@app.route('/get-query', methods=['GET','POST'])
def query_convert():
    statement=request.form.get('statement')
    country_name=[]
    place_entity = locationtagger.find_locations(text = statement)
    if place_entity.countries:
        country_name = place_entity.countries #---------Getting Country name from user query if mentioned--------------

    db=request.form.get("db")
    con=sqlite3.connect(db) #connecting to the database
    cursor=con.cursor()
    words_in_statement = word_tokenize(statement)
    pos=nltk.pos_tag(words_in_statement)
    print(pos)
    possible_columns=[]
    time=[]
    region=[]
    verb_possibility='no' #For getting better results for product names
    adjective_possibility='no'
    # -----------------------Words Segregation Starts---------------------------
    for i in pos:
        if i[1]=='VB'or i[1]=='VBD' or i[1]=='VBG' or i[1]=='VBN' or i[1]=='VBP' or i[1]=='VBZ':
            verb_possibility=i[0]
        if i[1]=='JJ' or i[1]=='JJR' or i[1]=='JJS':
            adjective_possibility=i[0]
        if i[1]=='NN' or i[1]=='NNP' or i[1]=='NNS':
            if verb_possibility!='no':
                possible_columns.append(verb_possibility)
            if adjective_possibility!='no':
                possible_columns.append(adjective_possibility)
            adjective_possibility='no'
            verb_possibility='no'
            possible_columns.append(i[0])
            region.append(i[0])
        if i[1]=='CD':
            verb_possibility='no'
            if int(i[0])>=1900:
                time.append(int(i[0]))
        if i[1]=='IN' or i[1]=='PRP':
            verb_possibility='no'
    # -----------------------Words Segregation End---------------------------
    date_column=[]
    x=cursor.execute('SELECT * FROM Dataset')
    columns=[]
    for column in x.description: # Getting the Column names
            columns.append(column[0])
    cursor.execute('SELECT DISTINCT product_name FROM Dataset')
    data=cursor.fetchall()
    pro={}
    vals={}
    countries=utils.get_countries(db) #----------------Creation of a list of unique countries from dataset-------------
    cities= utils.get_cities(db) #----------------Creation of a list of unique cities from dataset-------------
    states= utils.get_states(db) #----------------Creation of a list of unique states from dataset-------------
    city_name=[]
    state_name=[]
    for i in region:
        if i.capitalize() in cities:
            city_name.append(i) #---------------Fetching City name mentioned in user query---------
        if i.capitalize() in states:
            state_name.append(i)
        # else:
        #     for j in states:
        #         if i.lower()[0] == j.lower()[0]:
        #             similarity=utils.similar(i.capitalize(),j.capitalize())
        #             if similarity>0.7:
        #                 state_name.append(i)
    if not country_name: #------If country name not recognized using the library then compare--------
        for i in region:
            if i.capitalize() in countries:
                country_name.append(i)

#-----------Fetching column names from user query start----------------------
    for i in possible_columns:
        if len(i)>1:
            a=TextBlob(i)
            b=str(a.correct())
            print(b)
            vals[b]=[]
            pro[b]=[]
            for column in columns:
                if b.lower() in column.lower():
                    vals[b].append(column)
            if vals[b]==[]:
                del vals[b]
#-----------Fetching column names from user query end----------------------
        
#-----------Fetching products from user query start----------------------
            for product in data:
                if b.lower() in product[0].lower():
                    pro[b].append(product[0])
            if pro[b]==[]:
                del pro[b]
            val=list(pro.values())
            print(val)
    product=[]
    common_find=None
    if len(pro)>1: #----------Searching for common products from one or more nouns-----------
        for i in range(len(pro)-1):
            common_find=utils.common_member(val[i], val[i+1])

        if common_find==None:
            for i in range(len(pro)):
                product.append(val[i])
        else:
            common_find=list(common_find)
#-----------Fetching products from user query end----------------------

    for i in columns: #----------finding date related columns-------------
        if 'date' in i.lower():
            date_column.append(i)

    date_columns=str(date_column[0])
    print(vals,common_find)
    if vals and common_find:
        for i in vals.values():
            v=i[0] #----------------here v refers to column variable found in user query------------
            print (i,v)
        final_details={}
        if time!=[]:
            for i in range(min(time),max(time)+1):
                    if i not in time:
                        time.append(i) #-----here time contains all the years in the range entered by user------
        rows=[]
        comments=[]
        product_month_wise={}
        check_for_query_location_elements={"country":False, "state":False, "city":False}
        product_wise_locations={}
        for pros in common_find:
            final_month_wise={}
            empty_year=[]
            data_year=[]
            data=[]
            chart_rows=[]
            if country_name: #----if there is country name------
                check_for_query_location_elements['country']=True
                for i in country_name:
                    print(country_name)
                    if state_name: #-----if there is state name------
                        check_for_query_location_elements['state']=True
                        if city_name: #-----if there is city name------
                            check_for_query_location_elements['city']=True
                            for k in city_name:
                                print(city_name)
                                cursor.execute('SELECT '+v+', '+date_columns+' FROM Dataset WHERE product_name= ? AND order_city = ? AND order_country = ? ;',(pros, k.capitalize(), i.capitalize()))
                                data.append(list(cursor.fetchall()))
                        else:
                            for j in state_name:
                                print(state_name)
                                cursor.execute('SELECT '+v+', '+date_columns+', order_city FROM Dataset WHERE product_name= ? AND order_state = ? AND order_country = ? ;',(pros, j.capitalize(), i.capitalize()))
                                data.append(list(cursor.fetchall()))
                        
                    else: #------if not state name--------
                        if city_name: #-----if there is city name------
                            check_for_query_location_elements['city']=True
                            for k in city_name:
                                print(city_name)
                                cursor.execute('SELECT '+v+', '+date_columns+' FROM Dataset WHERE product_name= ? AND order_city = ? AND order_country = ? ;',(pros, k.capitalize(), i.capitalize()))
                                data.append(list(cursor.fetchall()))
                        else:#-----if not city name-----
                            cursor.execute('SELECT '+v+', '+date_columns+', order_state FROM Dataset WHERE product_name= ? AND order_country = ?',(pros, i.capitalize()))
                            data.append(list(cursor.fetchall()))
            else: #-------if not country name
                if state_name: #-----if there is state name------
                        check_for_query_location_elements['state']=True
                        if city_name: #-----if there is city name------
                                check_for_query_location_elements['city']=True
                                for j in city_name:
                                    print(city_name)
                                    cursor.execute('SELECT '+v+', '+date_columns+' FROM Dataset WHERE product_name= ? AND order_city = ? ;',(pros, j.capitalize()))
                                    data.append(list(cursor.fetchall()))
                        else:
                            for j in state_name:
                                print(state_name)
                                cursor.execute('SELECT '+v+', '+date_columns+', order_city FROM Dataset WHERE product_name= ? AND order_state = ?;',(pros, j.capitalize()))
                                data.append(list(cursor.fetchall()))
                        
                else: #------if not state name--------
                    if city_name: #-----if there is city name------
                        check_for_query_location_elements['city']=True
                        for k in city_name:
                            print(city_name)
                            cursor.execute('SELECT '+v+', '+date_columns+' FROM Dataset WHERE product_name= ? AND order_city = ?',(pros, j.capitalize()))
                            data.append(list(cursor.fetchall()))
                    else:
                        cursor.execute('SELECT '+v+', '+date_columns+', order_country FROM Dataset WHERE product_name= ? ',(pros,))
                        data.append(cursor.fetchall())
            print(check_for_query_location_elements)
            # print(pros, data[0])
            year_wise={}
            month_wise={}
            locations={}
            if data!=[[]]: #----------if we've got some data for our query for a product-----------
                for details in data[0]:
                        
                    date=pd.to_datetime(details[1]) #----------fetching out order date----------
                    if time != []:
                        if date.year not in time: #-- if date of the order is not in the timeframe asked by user discard it----
                            data[0].remove(details)

                            continue
                    #-----appending data in a dictionary yearwise eg {2016:[1,2,3]}---
                    if (check_for_query_location_elements['country']!=False or check_for_query_location_elements['state']!=False) and check_for_query_location_elements['city']==False:
                        if date.year in locations.keys():
                            if details[2] in locations[date.year]:
                                locations[date.year][details[2]]+=1
                            else:
                                locations[date.year][details[2]]=1
                        else:
                            locations[date.year]={}
                            locations[date.year][details[2]]=1
                    if (check_for_query_location_elements['country']!=False or check_for_query_location_elements['state']!=False) and check_for_query_location_elements['city']!=False:
                        locations=None
                    else:
                        if date.year in locations.keys():
                            if details[2] in locations[date.year]:
                                locations[date.year][details[2]]+=1
                            else:
                                locations[date.year][details[2]]=1
                        else:
                            locations[date.year]={}
                            locations[date.year][details[2]]=1
                    if date.year in year_wise:
                        year_wise[date.year].append(details[0])
                    else:
                        year_wise[date.year]=[details[0]]
                    
                    if str(date.month)+"."+str(date.year) in month_wise:
                        month_wise[str(date.month)+"."+str(date.year)].append(details[0])
                    else:
                        month_wise[str(date.month)+"."+str(date.year)]=[details[0]]
                product_wise_locations[pros]=locations
                months={'1':'January','2':'February','3':'March','4':'April','5':'May','6':'June','7':'July','8':'August','9':'September','10':'October','11':'November','12':'December'}
                for i in year_wise.keys():
                    year_under_consideration=list(year_wise[i])
                    sum_of_data=sum(year_under_consideration)
                    total_data=len(year_under_consideration)
                    finall=sum_of_data / total_data
                    year_wise[i]=finall #----yearwise data converted to avg of the collected data------
                for i in month_wise.keys():
                    month_under_consideration=month_wise[i]
                    if month_under_consideration:
                        finall=sum(month_under_consideration)/len(month_under_consideration)
                    else:
                        finall=0
                    month_wise[i]=finall #----yearwise data converted to avg of the collected data------
                    month, year=i.split('.')
                    if str(year) in final_month_wise.keys():
                        final_month_wise[str(year)][months[month]]=abs(finall)
                    else:
                        final_month_wise[str(year)]={months[month]:abs(finall)}
                print(final_month_wise)
                chart_rows.append(pros)
                no_time=False
                if time!=[]: #------in case no data for a year in that timeframe then avg value is 0------
                    for i in time:
                        if i not in list(year_wise.keys()):
                            year_wise[i]=0
                            empty_year.append(i)
                        else:
                            data_year.append(i)
                else:
                    no_time=True
                if len(empty_year)==1:
                    comments.append("No data found for "+pros+" for the year "+str(empty_year[0]))
                elif len(empty_year)>1:
                    message="No data found for "+pros+" for the years "
                    for i in range(len(empty_year)-1):
                        if i==0:
                            message=message+str(empty_year[i])
                        else:
                            message=message+", "+str(empty_year[i])
                    message=message+" and "+str(empty_year[-1])
                    comments.append(message)
                if not no_time:
                    if len(data_year)>1:
                        min_year=year_wise[min(data_year)]
                        max_year=year_wise[max(data_year)]
                        percentage_change=((min_year-max_year)/min_year) * 100
                        if percentage_change<0:
                            message="There has been a increase in "+v+" of "+pros+" by "+str(abs(percentage_change))+" percentage from year "+str(min(data_year))+" to year "+str(max(data_year))
                            comments.append(message)
                        elif percentage_change>0:
                            message="There has been a decrease in "+v+" of "+pros+" by "+str(abs(percentage_change))+" percentage from year "+str(min(data_year))+" to year "+str(max(data_year))
                            comments.append(message)
                    print(comments)
                else:
                    if len(year_wise)>1:
                        years=list(year_wise.keys())
                        min_year=year_wise[min(years)]
                        max_year=year_wise[max(years)]
                        percentage_change=((min_year-max_year)/min_year) * 100
                        if percentage_change<0:
                            message="There has been a increase in "+v+" of "+pros+" by "+str(abs((percentage_change)))+" percentage from year "+str(min(years))+" to year "+str(max(years))
                            comments.append(message)
                        elif percentage_change>0:
                            message="There has been a decrease in "+v+" of "+pros+" by "+str(abs(percentage_change))+" percentage from year "+str(min(years))+" to year "+str(max(years))
                            comments.append(message)
                year_wise_sorted={}
                while year_wise!={}: #----sorting yearwisedata in a new dict year_wise_Sorted-----
                    year_wise_sorted[min(year_wise.keys())]=year_wise[min(year_wise.keys())]
                    year_wise.pop(min(year_wise.keys()))

                for i in year_wise_sorted.keys():
                    chart_rows.append(year_wise_sorted[i])
                    if no_time==True:
                        time.append(i)
                product_month_wise[pros]=final_month_wise
                print(product_month_wise)
                final_details[pros]=year_wise_sorted
                rows.append(chart_rows)

            else:
                continue
        time_sort=sorted(time)
        return {'columns':vals,'products':final_details,'time':time_sort,'rows':rows, 'month':product_month_wise, 'comments':comments, 'location':product_wise_locations }

    elif vals and not common_find and pro:
        for i in vals.values():
            v=i[0]
        final_details={}
        if time!=[]:
            for i in range(min(time),max(time)+1):
                    if i not in time:
                        time.append(i)
        rows=[]
        for pros in pro.values():
            chart_rows=[]
            cursor.execute('SELECT '+v+', '+date_columns+' FROM Dataset WHERE product_name= ? ',(pros[0],))
            data=list(cursor.fetchall())
            
            year_wise={}
            for details in data:
                date=pd.to_datetime(details[1])
                if time!=[]:
                    if date.year not in time:
                        data.remove(details)
                        continue
            
                if date.year in year_wise:
                    year_wise[date.year].append(details[0])
                else:
                    year_wise[date.year]=[details[0]]
                        
            for i in year_wise.keys():
                year_under_consideration=year_wise[i]
                finall=sum(year_under_consideration) / len(year_under_consideration)
                year_wise[i]=finall
            no_time=False
            if time!=[]:
                for i in time:
                    if i not in list(year_wise.keys()):
                        year_wise[i]=0
            else:
                no_time=True
            year_wise_sorted={}
            while year_wise!={}:
                year_wise_sorted[min(year_wise.keys())]=year_wise[min(year_wise.keys())]
                year_wise.pop(min(year_wise.keys()))
            chart_rows.append(pros[0])
                
            for i in year_wise_sorted.keys():
                chart_rows.append(year_wise_sorted[i])
                if no_time==True:
                    time.append(i)
            
            final_details[pros[0]]=year_wise
            rows.append(chart_rows)
        time_sort=sorted(time)
        return {'columns':vals,'products':final_details,'time':time_sort,'rows':rows}
    else:
        return {'columns':None,'products':None,'time':None,'rows':None}


if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)