import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd

app = dash.Dash(__name__)

# Load data from a CSV file
df_top10_most_reviewed_business = pd.read_csv('s3://cs3103-yelp-raw/top10_most_reviewed_business.csv')
df_top10_business_review = pd.read_csv('s3://cs3103-yelp-raw/top10_business_review.csv')
df_top10_result_review = pd.read_csv('s3://cs3103-yelp-raw/top10_result_review.csv')
df_city_review = pd.read_csv('s3://cs3103-yelp-raw/top10_city_review.csv')
df_city_business_count = pd.read_csv('s3://cs3103-yelp-raw/top10_city_business_count.csv')
df_business_stars_dist = pd.read_csv('s3://cs3103-yelp-raw/business_stars_dist.csv')
df_sentiment_dist = pd.read_csv('s3://cs3103-yelp-raw/sentiment_dist.csv')
df_review_stars_dist = pd.read_csv('s3://cs3103-yelp-raw/review_stars_dist.csv')
df_nb_result = pd.read_csv('s3://cs3103-yelp-raw/result.csv')

complement_accuracy = 1 - df_nb_result.loc[0, 'accuracy']
error_data = {
    'Unnamed: 0': [1],
    'model': ['Error'],
    'accuracy': [complement_accuracy]
}
df_nb_result = pd.concat([df_nb_result, pd.DataFrame(error_data)], ignore_index=True)

fig_nb_result = px.pie(df_nb_result, names='model', values='accuracy',
                            title='Model Accuracy Distribution',
                            color_discrete_sequence=px.colors.sequential.RdBu)

fig_business_stars_dist = px.bar(df_business_stars_dist, x='business_stars', y='business_stars_total',
                                 title='Distribution of Business Stars',
                                 labels={'business_stars': 'Business Stars', 'business_stars_total': 'Total'},
                                 color='business_stars_total',
                                 color_continuous_scale=px.colors.sequential.Viridis)

fig_sentiment_dist = px.bar(df_sentiment_dist, x='sentiment', y='sentiment_total',
                            title='Distribution of Sentiments',
                            labels={'sentiment': 'Sentiment', 'sentiment_total': 'Total'},
                            color='sentiment_total',
                            color_continuous_scale=px.colors.sequential.Inferno)

fig_review_stars_dist = px.bar(df_review_stars_dist, x='review_stars', y='review_stars_total',
                               title='Distribution of Review Stars',
                               labels={'review_stars': 'Review Stars', 'review_stars_total': 'Total'},
                               color='review_stars_total',
                               color_continuous_scale=px.colors.sequential.Plasma)

fig_city_review = px.bar(df_city_review, x='city', y='business_stars_total',
                          title='Top 10 Cities by Business Stars',
                          labels={'business_stars_total': 'Business Stars Total', 'city': 'City'},
                          color='business_stars_total',
                          color_continuous_scale=px.colors.sequential.Plasma)

fig_city_business_count = px.bar(df_city_business_count, x='city', y='count',
                         title='Top 10 Cities by Business Count',
                         labels={'count': 'Business Count', 'city': 'City'},
                         color='count',
                         color_continuous_scale=px.colors.sequential.Cividis)

fig_top10_result_review = px.bar(df_top10_result_review, x='name', y='review_stars_total',
                          title='Top 10 Businesses by Review Stars',
                          labels={'review_stars_total': 'Review Stars Total', 'name': 'Business Name'},
                          color='review_stars_total',
                          color_continuous_scale=px.colors.sequential.Inferno)

fig_top10_business_review = px.bar(df_top10_business_review, x='name', y='business_stars_total',
                     title='Top 10 Businesses by Review Stars',
                     labels={'business_stars_total': 'Total Review Stars', 'name': 'Business Name'},
                     color='business_stars_total',
                     color_continuous_scale=px.colors.sequential.Viridis)

fig_top10_most_reviewed_business = px.bar(df_top10_most_reviewed_business, x='name', y='review_count', color='city', 
             title='Top 10 Most Reviewed Businesses',
             labels={'review_count': 'Review Count', 'name': 'Business Name', 'city': 'City'})

# Set up the layout of the Dash app
app.layout = html.Div(children=[
    html.H1(children='Business Data Visualization'),
    html.H2(children='Business Review Visualization'),
    # html.Div(children='A simple visualization for displaying review counts of businesses.'),
    dcc.Graph(
        id='business-graph',
        figure=fig_top10_most_reviewed_business
    ),
    html.H2(children='Top 10 Most Reviewed Businesses'),
    dcc.Graph(
        id='business-review-graph',
        figure=fig_top10_business_review
    ),
    html.H2(children='Top 10 Businesses by Review Stars'),
    dcc.Graph(
        id='review-stars-graph',
        figure=fig_top10_result_review
    ),
    html.H2(children='Top 10 Cities by Business Stars'),
    dcc.Graph(
        id='city-reviews-graph',
        figure=fig_city_review
    ),

    html.H2(children='Top 10 Cities by Business Count'),
    dcc.Graph(
        id='city-counts-graph',
        figure=fig_city_business_count
    ),
    html.H2(children='Distribution of Business Stars'),
    dcc.Graph(
        id='business-stars-dist-graph',
        figure=fig_business_stars_dist
    ),

    html.H2(children='Distribution of Sentiments'),
    dcc.Graph(
        id='sentiment-dist-graph',
        figure=fig_sentiment_dist
    ),

    html.H2(children='Distribution of Review Stars'),
    dcc.Graph(
        id='review-stars-dist-graph',
        figure=fig_review_stars_dist
    ),
    html.H2(children='Naive Bayes Classification Model Performance Visualization'),
    dcc.Graph(
        id='model-accuracy-pie-chart',
        figure=fig_nb_result
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
    # app.run_server(host= ‘0.0.0.0’,port=8050)