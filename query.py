import os
import re
import openai
import joblib
import pandas as pd

# load model
if not os.path.exists('taxi_trip_duration_model.pkl'):
    print("Error: Model file 'taxi_trip_duration_model.pkl' not found! Did you run train_model.py?")
    exit()
model = joblib.load('taxi_trip_duration_model.pkl')

if not os.path.exists('feature_order.pkl'):
    print("Error: Feature order file 'feature_order.pkl' not found! Did you run train_model.py?")
    exit()
feature_order = joblib.load('feature_order.pkl')

openai.api_key = "" 

def chat_with_gpt(query):
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "user", "content": query}
        ],
        max_tokens=150,
        temperature=0.7
    )
    return response['choices'][0]['message']['content'].strip()


def extract_features_from_query(query):
    days_of_week = {
        'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
        'Friday': 4, 'Saturday': 5, 'Sunday': 6
    }
    pickup_day_of_week = None
    for day, num in days_of_week.items():
        if day.lower() in query.lower():
            pickup_day_of_week = num
            break

    hour_match = re.search(r"(\d{1,2})\s?(AM|PM)", query, re.IGNORECASE)
    pickup_hour = None
    if hour_match:
        pickup_hour = int(hour_match.group(1))
        if hour_match.group(2).lower() == 'pm' and pickup_hour != 12:
            pickup_hour += 12

    taxi_types = ['yellow', 'green', 'fhvhv']
    taxi_type = None
    for t_type in taxi_types:
        if t_type in query.lower():
            taxi_type = t_type
            break

    return pickup_day_of_week, pickup_hour, taxi_type

def get_predicted_trip_duration(pickup_day_of_week, pickup_hour, taxi_type=None):

    features = {
        'pickup_day_of_week': [pickup_day_of_week],
        'pickup_hour': [pickup_hour],
        'taxi_type_yellow': [0],
        'taxi_type_green': [0],
        'taxi_type_fhvhv': [0]
    }

    if taxi_type:
        taxi_column = f"taxi_type_{taxi_type}"
        if taxi_column in features:
            features[taxi_column] = [1]

    features_df = pd.DataFrame(features)

    features_df = features_df[feature_order]

    print("Features being passed to model:\n", features_df)

    predicted_duration = model.predict(features_df)
    return predicted_duration[0]

def process_query(query):
    pickup_day_of_week, pickup_hour, taxi_type = extract_features_from_query(query)

    if taxi_type:
        print(f"Taxi Type: {taxi_type} found in query")
    else:
        print("No taxi type found, proceeding without it.")

    if any(word in query.lower() for word in ["trip duration", "how long", "estimate", "prediction"]):
        if pickup_day_of_week is not None and pickup_hour is not None:
            predicted_duration = get_predicted_trip_duration(pickup_day_of_week, pickup_hour, taxi_type)
            response = f"The predicted trip duration is {predicted_duration:.2f} minutes."
        else:
            response = "Sorry, I couldn't extract the necessary details for a prediction."
    elif "average wait time" in query.lower():
        response = "The average wait time for a taxi in New York City is approximately 10-15 minutes."
    else:
        response = chat_with_gpt(query)

    return response

# Example usage
if __name__ == "__main__":
    while True:
        query = input("Enter your query (or type 'exit' to quit): ")
        
        # If the user types 'exit', break the loop and end the program
        if query.lower() == 'exit':
            print("Exiting... Goodbye!")
            break

        # Process the query and print the response
        response = process_query(query)
        print(response)
