import requests
from datetime import datetime, timedelta
import json
import sys

# RUN THIS:
# python src/database/test_api.py

API_BASE_URL = "https://ooxcscccfhtawrjopkob.supabase.co/rest/v1"
API_KEY = "***REMOVED***"

headers = {
    "apikey": API_KEY,
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

test_results = []

def test(name, url, method="GET", params=None, data=None, expected_status=200, should_fail=False, description="", custom_headers=None):
    request_headers = custom_headers if custom_headers else headers
    try:
        if method == "GET":
            response = requests.get(url, headers=request_headers, params=params, timeout=10)
        elif method == "POST":
            response = requests.post(url, headers=request_headers, params=params, json=data, timeout=10)
        elif method == "PATCH":
            response = requests.patch(url, headers=request_headers, params=params, json=data, timeout=10)
        elif method == "DELETE":
            response = requests.delete(url, headers=request_headers, params=params, timeout=10)
        
        success = (response.status_code == expected_status) if not should_fail else (response.status_code != 200)
        
        result = {
            "name": name,
            "success": success,
            "status": response.status_code,
            "description": description,
            "url": url,
            "method": method
        }
        
        if success:
            try:
                result["data"] = response.json()
                if isinstance(result["data"], list):
                    result["count"] = len(result["data"])
                else:
                    result["count"] = 1
            except:
                result["response_text"] = response.text[:200]
        else:
            result["response_text"] = response.text[:500]
        
        status_icon = "[PASS]" if success else "[FAIL]"
        count_info = f" ({result.get('count', 'N/A')} records)" if "count" in result else ""
        print(f"  {status_icon} {name}{count_info}")
        
        test_results.append(result)
        return result
        
    except Exception as e:
        result = {
            "name": name,
            "success": False,
            "error": str(e),
            "description": description,
            "url": url,
            "method": method
        }
        print(f"  [FAIL] {name} - Error: {str(e)[:50]}")
        test_results.append(result)
        return result

def main():
    print("=" * 80)
    print("NBA PLAYER PERFORMANCE PREDICTION API - COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    print()
    
    print("SECTION 1: BASIC ENDPOINT TESTS")
    print("-" * 80)
    
    test("Get Players (limit 5)", f"{API_BASE_URL}/players", params={"limit": 5, "select": "player_id,full_name,team_id,position"})
    test("Get Teams (limit 5)", f"{API_BASE_URL}/teams", params={"limit": 5, "select": "team_id,abbreviation,full_name"})
    test("Get Games (limit 5)", f"{API_BASE_URL}/games", params={"limit": 5, "select": "game_id,game_date,game_status"})
    test("Get Predictions (limit 5)", f"{API_BASE_URL}/predictions", params={"limit": 5, "select": "prediction_id,player_id,game_id,predicted_points"})
    test("Get Confidence Components (limit 5)", f"{API_BASE_URL}/confidence_components", params={"limit": 5, "select": "component_id,prediction_id,stat_name"})
    
    print("\nSECTION 2: QUERY OPERATORS - EQUALITY")
    print("-" * 80)
    
    test("Player ID equals", f"{API_BASE_URL}/players", params={"player_id": "eq.2544", "select": "player_id,full_name"})
    test("Team abbreviation equals", f"{API_BASE_URL}/teams", params={"abbreviation": "eq.LAL", "select": "team_id,abbreviation,full_name"})
    test("Game status equals", f"{API_BASE_URL}/games", params={"game_status": "eq.completed", "limit": 3})
    test("Model version equals", f"{API_BASE_URL}/predictions", params={"model_version": "eq.xgboost", "limit": 3})
    
    print("\nSECTION 3: QUERY OPERATORS - COMPARISON")
    print("-" * 80)
    
    test("Confidence score greater than or equal", f"{API_BASE_URL}/predictions", params={"confidence_score": "gte.80", "limit": 5})
    test("Confidence score less than", f"{API_BASE_URL}/predictions", params={"confidence_score": "lt.50", "limit": 5})
    test("Predicted points greater than", f"{API_BASE_URL}/predictions", params={"predicted_points": "gt.25", "limit": 5})
    test("Predicted points less than or equal", f"{API_BASE_URL}/predictions", params={"predicted_points": "lte.5", "limit": 5})
    
    print("\nSECTION 4: QUERY OPERATORS - PATTERN MATCHING")
    print("-" * 80)
    
    test("Player name like (case-insensitive)", f"{API_BASE_URL}/players", params={"full_name": "ilike.*LeBron*", "select": "player_id,full_name"})
    test("Player name like (case-sensitive)", f"{API_BASE_URL}/players", params={"full_name": "like.*James*", "select": "player_id,full_name"})
    test("Team abbreviation like", f"{API_BASE_URL}/teams", params={"abbreviation": "ilike.L*", "select": "team_id,abbreviation"})
    
    print("\nSECTION 5: QUERY OPERATORS - IN OPERATOR")
    print("-" * 80)
    
    test("Player ID in list", f"{API_BASE_URL}/players", params={"player_id": "in.(2544,201935,203081)", "select": "player_id,full_name"})
    test("Team ID in list", f"{API_BASE_URL}/teams", params={"team_id": "in.(1610612747,1610612737)", "select": "team_id,abbreviation"})
    test("Model version in list", f"{API_BASE_URL}/predictions", params={"model_version": "in.(xgboost,lightgbm)", "limit": 5})
    
    print("\nSECTION 6: SORTING AND ORDERING")
    print("-" * 80)
    
    test("Predictions ordered by points DESC", f"{API_BASE_URL}/predictions", params={"order": "predicted_points.desc", "limit": 5})
    test("Predictions ordered by points ASC", f"{API_BASE_URL}/predictions", params={"order": "predicted_points.asc", "limit": 5})
    test("Predictions ordered by confidence DESC", f"{API_BASE_URL}/predictions", params={"order": "confidence_score.desc,predicted_points.desc", "limit": 5})
    test("Players ordered by name ASC", f"{API_BASE_URL}/players", params={"order": "full_name.asc", "limit": 5})
    
    print("\nSECTION 7: COLUMN SELECTION")
    print("-" * 80)
    
    test("Select specific columns - predictions", f"{API_BASE_URL}/predictions", params={"select": "prediction_id,player_id,predicted_points", "limit": 5})
    test("Select specific columns - players", f"{API_BASE_URL}/players", params={"select": "player_id,full_name,position", "limit": 5})
    test("Select specific columns - confidence components", f"{API_BASE_URL}/confidence_components", params={"select": "component_id,stat_name,calibrated_score", "limit": 5})
    
    print("\nSECTION 8: PAGINATION")
    print("-" * 80)
    
    test("Pagination - limit 10", f"{API_BASE_URL}/predictions", params={"limit": 10})
    test("Pagination - limit 1", f"{API_BASE_URL}/predictions", params={"limit": 1})
    test("Pagination - limit and offset", f"{API_BASE_URL}/predictions", params={"limit": 5, "offset": 10})
    
    print("\nSECTION 9: DATE FILTERING")
    print("-" * 80)
    
    today = datetime.now().strftime("%Y-%m-%d")
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    month_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    test("Games on specific date", f"{API_BASE_URL}/games", params={"game_date": f"eq.{week_ago}", "limit": 5})
    test("Games since date", f"{API_BASE_URL}/games", params={"game_date": f"gte.{week_ago}", "limit": 5})
    test("Games before date", f"{API_BASE_URL}/games", params={"game_date": f"lte.{month_ago}", "limit": 5})
    test("Predictions for specific date", f"{API_BASE_URL}/predictions", params={"prediction_date": f"eq.{week_ago}", "limit": 5})
    
    print("\nSECTION 10: COMBINED FILTERS")
    print("-" * 80)
    
    test("High confidence high points", f"{API_BASE_URL}/predictions", params={"confidence_score": "gte.75", "predicted_points": "gte.20", "limit": 5})
    test("Low confidence low points", f"{API_BASE_URL}/predictions", params={"confidence_score": "lt.60", "predicted_points": "lt.10", "limit": 5})
    test("Specific model and confidence", f"{API_BASE_URL}/predictions", params={"model_version": "eq.catboost", "confidence_score": "gte.70", "limit": 5})
    test("Active players on team", f"{API_BASE_URL}/players", params={"is_active": "eq.true", "team_id": "eq.1610612747", "limit": 5})
    
    print("\nSECTION 11: RELATIONSHIP QUERIES")
    print("-" * 80)
    
    test("Predictions for specific player", f"{API_BASE_URL}/predictions", params={"player_id": "eq.2544", "limit": 5})
    test("Predictions for specific game", f"{API_BASE_URL}/predictions", params={"game_id": "eq.0022500336", "limit": 5})
    test("Confidence components for prediction", f"{API_BASE_URL}/confidence_components", params={"prediction_id": "eq.571", "limit": 5})
    test("Games for specific team (home)", f"{API_BASE_URL}/games", params={"home_team_id": "eq.1610612747", "limit": 5})
    test("Games for specific team (away)", f"{API_BASE_URL}/games", params={"away_team_id": "eq.1610612747", "limit": 5})
    
    print("\nSECTION 12: EDGE CASES AND SPECIAL CHARACTERS")
    print("-" * 80)
    
    test("Player with special characters", f"{API_BASE_URL}/players", params={"full_name": "ilike.*Bogdan*", "select": "player_id,full_name"})
    test("Empty result set (invalid player)", f"{API_BASE_URL}/players", params={"player_id": "eq.99999999"})
    test("Empty result set (future date)", f"{API_BASE_URL}/predictions", params={"prediction_date": "eq.2099-12-31"})
    test("Very high limit", f"{API_BASE_URL}/predictions", params={"limit": 100})
    
    print("\nSECTION 13: STATISTICS QUERIES")
    print("-" * 80)
    
    test("Predictions for steals", f"{API_BASE_URL}/predictions", params={"predicted_steals": "gte.2", "limit": 5})
    test("Predictions for blocks", f"{API_BASE_URL}/predictions", params={"predicted_blocks": "gte.2", "limit": 5})
    test("Predictions for assists", f"{API_BASE_URL}/predictions", params={"predicted_assists": "gte.10", "limit": 5})
    test("Predictions for rebounds", f"{API_BASE_URL}/predictions", params={"predicted_rebounds": "gte.10", "limit": 5})
    test("Predictions for three pointers", f"{API_BASE_URL}/predictions", params={"predicted_three_pointers_made": "gte.4", "limit": 5})
    
    print("\nSECTION 14: CONFIDENCE COMPONENTS QUERIES")
    print("-" * 80)
    
    test("Confidence components by stat", f"{API_BASE_URL}/confidence_components", params={"stat_name": "eq.points", "limit": 5})
    test("High ensemble score", f"{API_BASE_URL}/confidence_components", params={"ensemble_score": "gte.20", "limit": 5})
    test("Low variance score", f"{API_BASE_URL}/confidence_components", params={"variance_score": "lt.10", "limit": 5})
    test("Specific calibrated score range", f"{API_BASE_URL}/confidence_components", params={"calibrated_score": "gte.80", "limit": 5})
    
    print("\nSECTION 15: SECURITY TESTS - READ-ONLY VERIFICATION")
    print("-" * 80)
    
    test("POST should fail (write attempt)", f"{API_BASE_URL}/predictions", method="POST", data={"player_id": 999, "game_id": "test"}, should_fail=True, expected_status=405, description="Attempt to create prediction - should fail")
    test("PATCH should fail (update attempt)", f"{API_BASE_URL}/predictions/prediction_id=eq.1", method="PATCH", data={"predicted_points": 999}, should_fail=True, expected_status=405, description="Attempt to update prediction - should fail")
    test("DELETE should fail (delete attempt)", f"{API_BASE_URL}/predictions/prediction_id=eq.1", method="DELETE", should_fail=True, expected_status=405, description="Attempt to delete prediction - should fail")
    test("POST to players should fail", f"{API_BASE_URL}/players", method="POST", data={"player_id": 999, "full_name": "Test"}, should_fail=True, expected_status=405, description="Attempt to create player - should fail")
    
    print("\nSECTION 16: AUTHENTICATION TESTS")
    print("-" * 80)
    
    test("Missing API key", f"{API_BASE_URL}/players", custom_headers={"apikey": "", "Authorization": ""}, should_fail=True, expected_status=401, description="Request without authentication - should fail")
    test("Invalid API key", f"{API_BASE_URL}/players", custom_headers={"apikey": "invalid_key", "Authorization": "Bearer invalid_key"}, should_fail=True, expected_status=401, description="Request with invalid key - should fail")
    
    print("\nSECTION 17: INVALID QUERY TESTS")
    print("-" * 80)
    
    test("Invalid column name", f"{API_BASE_URL}/predictions", params={"invalid_column": "eq.5"}, should_fail=True, expected_status=400, description="Query with non-existent column - should fail")
    test("Invalid operator", f"{API_BASE_URL}/predictions", params={"player_id": "invalid_op.5"}, should_fail=True, expected_status=400, description="Query with invalid operator - should fail")
    test("Malformed date", f"{API_BASE_URL}/games", params={"game_date": "eq.invalid-date"}, should_fail=True, expected_status=400, description="Query with invalid date format - should fail")
    
    print("\nSECTION 18: PERFORMANCE AND LIMIT TESTS")
    print("-" * 80)
    
    test("Large result set", f"{API_BASE_URL}/predictions", params={"limit": 50})
    test("Single record", f"{API_BASE_URL}/predictions", params={"limit": 1})
    test("Complex query with multiple filters", f"{API_BASE_URL}/predictions", params={"model_version": "in.(xgboost,lightgbm,catboost)", "confidence_score": "gte.70", "predicted_points": "gte.15", "order": "predicted_points.desc", "limit": 10})
    
    print("\nSECTION 19: TABLE SPECIFIC TESTS - PLAYERS")
    print("-" * 80)
    
    test("Players by position", f"{API_BASE_URL}/players", params={"position": "ilike.*Guard*", "limit": 5})
    test("Active players only", f"{API_BASE_URL}/players", params={"is_active": "eq.true", "limit": 5})
    test("Inactive players only", f"{API_BASE_URL}/players", params={"is_active": "eq.false", "limit": 5})
    test("Players on specific team", f"{API_BASE_URL}/players", params={"team_id": "eq.1610612747", "limit": 5})
    
    print("\nSECTION 20: TABLE SPECIFIC TESTS - GAMES")
    print("-" * 80)
    
    test("Completed games", f"{API_BASE_URL}/games", params={"game_status": "eq.completed", "limit": 5})
    test("Scheduled games", f"{API_BASE_URL}/games", params={"game_status": "eq.scheduled", "limit": 5})
    test("Games in specific season", f"{API_BASE_URL}/games", params={"season": "eq.2024-25", "limit": 5})
    test("Games with scores", f"{API_BASE_URL}/games", params={"home_score": "not.is.null", "limit": 5})
    
    print("\nSECTION 21: TABLE SPECIFIC TESTS - PREDICTIONS")
    print("-" * 80)
    
    test("Predictions by model version", f"{API_BASE_URL}/predictions", params={"model_version": "eq.random_forest", "limit": 5})
    test("Predictions with actuals", f"{API_BASE_URL}/predictions", params={"actual_points": "not.is.null", "limit": 5})
    test("Predictions without actuals", f"{API_BASE_URL}/predictions", params={"actual_points": "is.null", "limit": 5})
    test("Predictions by all 4 models", f"{API_BASE_URL}/predictions", params={"model_version": "in.(xgboost,lightgbm,catboost,random_forest)", "limit": 10})
    
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    passed = sum(1 for r in test_results if r["success"])
    failed = len(test_results) - passed
    total = len(test_results)
    
    print(f"\nTotal Tests: {total}")
    print(f"Passed: {passed} ({passed/total*100:.1f}%)")
    print(f"Failed: {failed} ({failed/total*100:.1f}%)")
    
    if failed > 0:
        print("\nFAILED TESTS:")
        print("-" * 80)
        for result in test_results:
            if not result["success"]:
                print(f"✗ {result['name']}")
                if result.get("description"):
                    print(f"  {result['description']}")
                print(f"  {result['method']} {result['url']}")
                print(f"  Status: {result['status']}")
                if "error" in result:
                    print(f"  Error: {result['error']}")
                if "response_text" in result:
                    print(f"  Response: {result['response_text'][:200]}")
                print()
    
    print("\n" + "=" * 80)
    if passed == total:
        print("ALL TESTS PASSED!")
    else:
        print(f"{failed} TEST(S) FAILED")
    print("=" * 80)
    
    return passed == total

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
