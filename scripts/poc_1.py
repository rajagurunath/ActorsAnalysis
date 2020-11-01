import streamlit as st
import cv2
import os
import json
import numpy as np
import pandas as pd
import plotly.express as px
from pytrends.request import TrendReq
import altair as alt
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim


PATH=os.path.abspath(os.path.join('.','data'))
pytrend = TrendReq()

# st.set_option('wideMode', True)
# st.beta_set_page_config()



st.beta_set_page_config(layout="wide")

def find_lat_long(df):
    # declare an empty list to store
    # latitude and longitude of values
    # of city column
    longitude = []
    latitude = []

    # function to find the coordinate
    # of a given city
    def findGeocode(city):

        # try and catch is used to overcome
        # the exception thrown by geolocator
        # using geocodertimedout
        try:

            # Specify the user_agent as your
            # app name it should not be none
            geolocator = Nominatim(user_agent="your_app_name")

            return geolocator.geocode(city)

        except GeocoderTimedOut:

            return findGeocode(city)

            # each value from city column

    # will be fetched and sent to
    # function find_geocode
    for i in (df["geoName"]):

        if findGeocode(i) != None:

            loc = findGeocode(i)

            # coordinates returned from
            # function is stored into
            # two separate list
            latitude.append(loc.latitude)
            longitude.append(loc.longitude)

            # if coordinate for a city not
        # found, insert "NaN" indicating
        # missing value
        else:
            latitude.append(np.nan)
            longitude.append(np.nan)
    return latitude, longitude

def data_docs():

    st.title("How to create a Persona")
    st.markdown(
        """
        ### Persona Creation
    
        - Actors social media
            - Facebook
            - Twitter
            - instagram
            - Google Trends
            
        - Products
            - Related informations
            - Dono yet
        
        ### Data Vareity

        - Text Feeds (NLP Analysis)
        - Photo (Computer vision )
        - Trends (Time series)
        - MP4 files (speech analysis)

        ### Algorithms

        - Embeding Alogorithm 
            - Projection of all features into space (Where all related Properties will 
                stay close and different properties will stay further)
            - Embeding of Text , Vision ,some Trends data also
               
        - Recomendations
            - Marrying the Embedings 
            - Similarity Algorithms 
            - Matrix Factorization
            - Deep Neural Network
        """

    )


@st.cache
def gettwitterFrds(actor):
    twt_path = os.path.join(PATH, actor, 'twitter')
    with open(os.path.join(twt_path, 'friends.json'), 'r') as f:
        frds = json.load(f)

    return pd.DataFrame(frds)

@st.cache
def gettwitterTimeline(actor):
    twt_path = os.path.join(PATH, actor, 'twitter')
    with open(os.path.join(twt_path, 'timeline.json'), 'r') as f:
        time = json.load(f)

    return pd.DataFrame(time)

@st.cache
def gettwitterMeta(actor):
    twt_path=os.path.join(PATH,actor,'twitter')
    with open(os.path.join(twt_path, 'meta.json'), 'r') as f:
        user=json.load(f)
    # st.write(user)
    return user

def prepare_twitter_stats(actor):
    st.title("Actors persona using Twitter Data")

    actor = st.selectbox(
        "Choose Actor", os.listdir(PATH)
    )

    # st.subheader(f"Preparing stats for {actor}")
    user=gettwitterMeta(actor)

    st.header("Twitter Analytics")

    cols=st.beta_columns(2)
    with cols[0]:
        st.subheader(user['screen_name'])

        st.image(user['profile_image_url'])

        st.subheader("Tag Line : \n"+user['description'])
    with cols[1]:
        st.image(user['profile_banner_url'],use_column_width=True)
        st.subheader(f"Followers count: {user['followers_count']}")
        st.subheader(f"Friends count: {user['friends_count']}")

    frds=gettwitterFrds(actor)

    st.subheader("Followers in various Location")


    st.plotly_chart(
        px.scatter(frds,x='location',y='followers_count',size='followers_count',color='location'),
                    use_container_width=True)


    st.subheader("Friends in various Location")
    st.plotly_chart(px.scatter(frds, x='location', y='friends_count', size='friends_count', color='location'),
                    use_container_width=True)

    st.header(f"Some Tweets from {actor}")

    timeline=gettwitterTimeline(actor)

    for text in timeline['text']:
        st.write(text)

    show = st.selectbox("show Table", ["Yes", "No"])


    if show == "Yes":
        columns=st.multiselect("choose Relevant Information",frds.columns.tolist())
        st.table(frds[columns])

    return

@st.cache
def load_instagram_data(actor):
    with st.spinner("Loading data"):
        with open(os.path.join(PATH,actor,'meta.json'),'r') as f:
            meta=json.load(f)

    return meta
def prepare_instagram_stats(actor):

    st.title("Actors persona using Instagram")
    actor = st.selectbox(
        "Choose Actor", os.listdir(PATH)
    )
    st.subheader(f"Preparing stats for {actor}")
    st.balloons()
    imgPath=os.path.join(PATH,actor,"images")
    imgfilelist=os.listdir(imgPath)
    imgList=[]

    meta= load_instagram_data(actor)
    st.success(f"Meta data for {actor} loaded")

    cols = st.beta_columns(2)
    with cols[0]:
        st.subheader(f"{meta['0']['node.full_name']}")
        st.image(meta['0']['node.profile_pic_url'],use_column_width=True)

    with cols[1]:
        st.subheader(f"Followers: {meta['0']['node.edge_followed_by.count']}")
    st.subheader(f"Tagline: {meta['0']['node.biography']}")



    friendsDF=pd.json_normalize(meta['0']['node.edge_related_profiles.edges'])

    number_of_friends=min(10,friendsDF.shape[0])
    st.subheader(f"Close friends of {actor}")
    cols=st.beta_columns(number_of_friends)
    for i in range(number_of_friends):
        url=friendsDF.loc[i,"node.profile_pic_url"]
        with cols[i]:
            st.image(url,caption=friendsDF.loc[i,'node.username'],use_column_width=True)

    st.subheader(f"Pictures of {actor}")
    cols=st.beta_columns(15)

    for idx,imgf in enumerate(imgfilelist[-15:]):
        with cols[idx]:
            imgff=os.path.join(imgPath,imgf)
            img=cv2.imread(imgff)
            # st.write(img.shape)
            st.image(np.array(img),use_column_width=True)
    st.subheader(f"Post of {actor}")
    captions=pd.DataFrame(meta['captions'])
    for row in captions.iloc[::-1].iterrows():
        st.text(row[1]['caption'])
    show = st.selectbox("show Friend list", ["Yes", "No"])
    if show == "Yes":
        st.write(friendsDF)

    return

@st.cache
def prepare_relevant_data(kw_list):
    """

    Prepares relevant Topics &relevant Query dataframe used along with the kw_list

    :param kw_list:
    :return: (pd.DataFrame,pd.DataFrame)
    """
    pytrend.build_payload(kw_list=kw_list)
    relatedqueriesDF = pytrend.related_queries()
    relatedDF = pytrend.related_topics()
    return relatedDF,relatedqueriesDF

@st.cache
def prepare_trend_data(kw_list,with_cat_code=True):
    """

    :param kw_list: (keywords for which the trend as to be analysed
    :return: (pd.DataFrame,pd.DataFrame)
    """

    pytrend.build_payload(kw_list=kw_list)

    interest_by_regionDF = pytrend.interest_by_region().reset_index()
    interest_by_regionDF = interest_by_regionDF[interest_by_regionDF.iloc[:, 0] != 0]

    interest_by_timeDF = pytrend.interest_over_time().reset_index()
    if with_cat_code:
        with open("country_code_mapper.json",'r') as f:
            mapper=json.load(f)
        interest_by_regionDF['country_code']=interest_by_regionDF['geoName'].map(mapper)
    return interest_by_regionDF,interest_by_timeDF

def prepare_trend_stats(actor):
    st.title("Analysing Digital popularity of Product or Actors")
    st.subheader(f"Preparing Trend stats for {actor}")
    search_text = st.text_input("Choose Entity")
    suggestions = pytrend.suggestions(search_text)
    selected = st.selectbox("Suggestions", [f"{s['title']} - {s['type']}" for s in suggestions])
    st.subheader(f"preparing stats for {selected}")
    selectedName=selected.split("-")[0].strip()
    interest_by_regionDF,interest_by_timeDF=prepare_trend_data(kw_list=[selectedName])

    interest_by_regionDF = interest_by_regionDF[interest_by_regionDF.iloc[:, 1] != 0]
    # chart = alt.Chart(interest_by_regionDF.sort_values(by=selectedName, ascending=False).reset_index())
    # c1=chart.mark_bar().encode(alt.X("geoName"), alt.Y(selectedName, title="Freq of search"),
    #                         color='geoName').interactive()
    # st.altair_chart(c1)
    st.subheader(f"Popularity of {selectedName} by region ")
    # st.map(interest_by_regionDF)
    st.plotly_chart(px.bar(interest_by_regionDF,x='geoName',y=selectedName,color='geoName',
                           labels={selectedName:"Frequency of search"}),
                        use_container_width=True)
    w_chart = px.choropleth(interest_by_regionDF, locations="country_code",
                            color=selectedName,  # lifeExp is a column of gapminder
                            hover_name="geoName",  # column to add to hover information
                            color_continuous_scale=px.colors.sequential.Plasma)
    st.plotly_chart(w_chart,use_container_width=True)

    st.subheader(f"Popularity of {selectedName} by time ")
    st.plotly_chart(px.line(interest_by_timeDF, x='date', y=selectedName,
                            labels={selectedName:"Frequency of search"}),
                    use_container_width=True)

    st.subheader(f"How people well people know our {selectedName}")
    st.text(f"In the Nutshell, what are all the keywords people are using to search about {selectedName}")
    d1,d2=prepare_relevant_data([selected.split("-")[0].strip()])
    first_key = list(d1.keys())[0]

    st.header("Search Topics")
    st.subheader(f"Top search terms for {selected}")
    tmp=d1[first_key]['top'].sort_values(by='value').iloc[:10,:]
    fig=px.bar(tmp,x="topic_title",y="value",color='topic_type',)
    st.plotly_chart(fig,use_container_width=True)


    st.subheader(f"Rising search terms for {selected}")
    # d1[first_key]['rising']
    tmp = d1[first_key]['rising'].sort_values(by='value').iloc[:10,     :]
    fig = px.bar(tmp, x="topic_title", y="value", color='topic_type')
    st.plotly_chart(fig,use_container_width=True)

    st.header("Search Queries")
    st.subheader(f"Top Questions for {selected}")
    tmp = d2[first_key]['top'].sort_values(by='value').iloc[:10, :]
    fig = px.bar(tmp, x="query", y="value", )
    st.plotly_chart(fig,use_container_width=True)


    st.subheader(f"Rising Questions terms for {selected}")
    # d1[first_key]['rising']
    tmp = d2[first_key]['rising'].sort_values(by='value').iloc[:10, :]
    fig = px.bar(tmp, x="query", y="value",)
    st.plotly_chart(fig,use_container_width=True)

    show = st.selectbox("show relavent Topics", ["No", "Yes"])
    if show == "Yes":
        # d1,d2=prepare_relevant_data([selected.split("-")[0].strip()])

        st.subheader(f"Topics searched along with the {selected}")
        st.write(d1[first_key]['top'])
        st.write(d1[first_key]['rising'])
        st.subheader(f"Queries asked along with the {selected} in Google search")
        st.write(d2[first_key]['top'])
        st.write(d2[first_key]['rising'])


    return


def actors_persona(analysis):


    mapper={
        "Twitter":prepare_twitter_stats,
        "instagram":prepare_instagram_stats,
        "Digital presence": prepare_trend_stats,
    }
    mapper.get(analysis)(actor=None)

def comparison():
    st.title("Actors Comparison")

st.sidebar.title("InvestTech 💘")
app_mode = st.sidebar.selectbox("Lets Explore !!!",
    ["About the data", "Actors persona", "Comparison"])
if app_mode == "About the data":
    st.sidebar.success('To continue select "Run the app".')
    data_docs()
if app_mode=="Actors persona":
    # st.sidebar.multiselect("choose Analysis",["Twitter","instagram","Digital persence"])
    analysis=st.sidebar.radio("choose Analysis",["Twitter","instagram","Digital presence"])
    actors_persona(analysis)

if app_mode=='Comparison':
    comparison()
    # elif app_mode == "Show the source code":
    #     readme_text.empty()
    #     st.code(get_file_content_as_string("app.py"))
    # elif app_mode == "Run the app":
    #     readme_text.empty()
    #     run_the_app()
    #

