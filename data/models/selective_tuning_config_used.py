SELECTIVE_TUNING_CONFIG = {
    'xgboost': {
        'blocks': True,
        'steals': True,
        'points': False,
        'assists': False,
        'rebounds': False,
        'turnovers': False,
        'three_pointers_made': False,
    },
    'lightgbm': {
        'blocks': False,
        'steals': False,
        'points': False,
        'assists': False,
        'rebounds': False,
        'turnovers': False,
        'three_pointers_made': False,
    },
    'catboost': {
        'blocks': True,
        'steals': True,
        'points': True,
        'assists': True,
        'rebounds': True,
        'turnovers': True,
        'three_pointers_made': True,
    },
    'random_forest': {
        'blocks': False,
        'steals': False,
        'points': False,
        'assists': False,
        'rebounds': False,
        'turnovers': False,
        'three_pointers_made': False,
    }
}

def should_use_tuned_params(model_type, target_name, use_selective=True):
    if not use_selective:
        return True
    
    if model_type not in SELECTIVE_TUNING_CONFIG:
        return False
    
    return SELECTIVE_TUNING_CONFIG[model_type].get(target_name, False)

