
import streamlit as st
import pickle as pkl
import numpy as np
import pandas as pd
import requests

# Define the form fields as per the HTML file
with open('best_randomforest_model.pkl', 'rb') as model_file:
    model = pkl.load(model_file)

with open('scaling.pkl', 'rb') as scaler_file:
    scaler = pkl.load(scaler_file)

def stock_price_prediction(input_data):
    data = pd.DataFrame([input_data])
    data_new=scaler.transform(data)
    prediction = model.predict(data_new)
    print(prediction)

    return prediction[0]

def main():

    st.title("Stock Price Prediction ")
    # getting the input data from the user
    st.subheader("**Enter values below**", )
    open_val=st.text_input('Open')
    high_val = st.text_input('High')
    low_val = st.text_input('Low')
    volume_val = st.text_input('Volume')


    # daily_ret_pct = st.text_input('Daily Return as %')
    # daily_var = st.text_input('Daily Variation')
    # macd = st.text_input('MACD')
    # rsi = st.text_input('RSI')
    # ema = st.text_input('EMA')

    # code for Prediction
    prediction = ''

    # creating a button for Prediction
    if st.button('Predict Stock Price'):
        input_data = [open_val,high_val,low_val,volume_val]

        if all(input_data):
            prediction = stock_price_prediction(input_data)
        else:
            prediction = 'Please fill in all fields'

    st.success(prediction)

if __name__ == '__main__':
    main()
