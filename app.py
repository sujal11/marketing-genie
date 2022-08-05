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
import locationtagger
from statistics import mean
import geopy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
  
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
    # Dataframe.to_sql(name='Dataset',con=con,if_exists='replace') # Dataset converted to RDBMS table

    # con.commit()
    
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
        reviews={'neg':0,'pos':0}
        for review in data:
            sid=SentimentIntensityAnalyzer()
            sa=sid.polarity_scores(str(review))
            if sa['neg']>sa['pos']:
                reviews['neg']=reviews['neg']+1
            else:
                reviews['pos']=reviews['pos']+1
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


if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)