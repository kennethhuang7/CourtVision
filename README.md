# NBA Player Performance Prediction

This project predicts how NBA players will perform in upcoming games. It uses machine learning to guess how many points, rebounds, assists, and other stats a player will have.

## What It Does

The project looks at many things to make predictions:

- How the player played in their last 5, 10, and 20 games
- Weighted averages that give more importance to recent games
- How good the player's team is
- How good the opponent's defense is
- How well the opponent defends against the player's position
- Whether the game is at home or away
- How many days of rest the player had
- Whether it's a back to back game
- The player's career and season experience

By combining multiple models together, we get better predictions than using just one model. This reduces the error rate compared to using individual models.

## Setup

1. Install Python 3.8 or higher

2. Create a virtual environment:
```
python -m venv venv
```

3. Activate the virtual environment:
```
source venv/bin/activate
```

4. Install the required packages:
```
pip install -r requirements.txt
```

5. Set up your database connection in a .env file

## How to Use

### Build Features

To create the features used for training:
```
python src/feature_engineering/build_features.py
```

### Train Models

To train the prediction models:
```
python src/models/train_models.py
```

### Make Predictions

To predict upcoming games:
```
python src/predictions/predict_games.py
```

### Run the Web App

To start the Streamlit web interface:
```
streamlit run streamlit_app/Home.py
```

## Project Structure

- `src/data_collection/` - Code to get NBA data
- `src/feature_engineering/` - Code to create features from the data
- `src/models/` - Code to train and save models
- `src/predictions/` - Code to make predictions
- `src/evaluation/` - Code to test model performance
- `streamlit_app/` - Web interface for viewing predictions
- `data/models/` - Saved trained models
- `data/evaluation/` - Model performance results

## Models Used

The system uses four different machine learning models:
- XGBoost
- LightGBM
- CatBoost
- Random Forest

Predictions from all models are averaged together to get the final prediction.

## Features

The project creates many features from player and game data:
- Rolling averages from recent games
- Weighted averages that favor recent performance
- Team strength ratings
- Opponent defensive stats
- Position specific defense stats
- Game context like home/away and rest days
- Career and season experience

