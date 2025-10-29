"""
Great Expectations Validator
Validates batch and streaming data
"""

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import os
import json


class DataValidator:
    """Handles data validation using Great Expectations"""
    
    def __init__(self):
        """Initialize GE context"""
        self.context = gx.get_context()
        self._setup_datasources()
    
    def _setup_datasources(self):
        """Setup GE datasources"""
        # Pandas datasource for runtime data
        datasource_config = {
            "name": "runtime_datasource",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "PandasExecutionEngine"
            },
            "data_connectors": {
                "default_runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"]
                }
            }
        }
        
        try:
            self.context.get_datasource("runtime_datasource")
        except:
            self.context.add_datasource(**datasource_config)
    
    def validate_ratings_batch(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate ratings data (train.csv, test.csv)
        
        Args:
            df: DataFrame with columns [user_idx, movie_idx, rating, timestamp]
            
        Returns:
            Validation results dictionary
        """
        suite_name = "ratings_suite"
        
        # Create or get suite
        try:
            suite = self.context.get_expectation_suite(suite_name)
        except:
            suite = self.context.add_expectation_suite(suite_name)
        
        # Create batch
        batch_request = RuntimeBatchRequest(
            datasource_name="runtime_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="ratings",
            runtime_parameters={"batch_data": df},
            batch_identifiers={
                "default_identifier_name": f"ratings_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            }
        )
        
        # Get validator
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        # Define expectations
        self._add_ratings_expectations(validator)
        
        # Run validation
        results = validator.validate()
        
        # Parse results
        return self._parse_results(results)
    
    def validate_movies_batch(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate movies data (movies_clean.csv)
        
        Args:
            df: DataFrame with columns [movie_idx, movieId, title, genres]
            
        Returns:
            Validation results dictionary
        """
        suite_name = "movies_suite"
        
        # Create or get suite
        try:
            suite = self.context.get_expectation_suite(suite_name)
        except:
            suite = self.context.add_expectation_suite(suite_name)
        
        # Create batch
        batch_request = RuntimeBatchRequest(
            datasource_name="runtime_datasource",
            data_connector_name="default_runtime_data_connector",
            data_asset_name="movies",
            runtime_parameters={"batch_data": df},
            batch_identifiers={
                "default_identifier_name": f"movies_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            }
        )
        
        # Get validator
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        # Define expectations
        self._add_movies_expectations(validator)
        
        # Run validation
        results = validator.validate()
        
        # Parse results
        return self._parse_results(results)
    
    def validate_stream_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a single streaming event
        
        Args:
            event: Event dictionary with fields
            
        Returns:
            Validation results
        """
        errors = []
        
        # Required fields
        required_fields = ['user_idx', 'movie_idx', 'event_type', 'timestamp']
        for field in required_fields:
            if field not in event:
                errors.append(f"Missing required field: {field}")
        
        if errors:
            return {
                "valid": False,
                "errors": errors,
                "event": event
            }
        
        # Validate event_type
        valid_event_types = ['rating', 'view', 'click', 'search']
        if event['event_type'] not in valid_event_types:
            errors.append(f"Invalid event_type: {event['event_type']}")
        
        # Validate rating if present
        if event['event_type'] == 'rating':
            if 'rating' not in event:
                errors.append("Rating event missing 'rating' field")
            elif not (0.5 <= event['rating'] <= 5.0):
                errors.append(f"Rating out of range: {event['rating']}")
            elif event['rating'] not in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]:
                errors.append(f"Rating not in 0.5 increments: {event['rating']}")
        
        # Validate indices
        if event.get('user_idx', -1) < 0:
            errors.append(f"Invalid user_idx: {event.get('user_idx')}")
        
        if event.get('movie_idx', -1) < 0:
            errors.append(f"Invalid movie_idx: {event.get('movie_idx')}")
        
        # Validate timestamp
        current_time = datetime.now().timestamp()
        event_time = event.get('timestamp', 0)
        
        if event_time > current_time + 60:  # Max 60 sec in future (clock skew)
            errors.append(f"Timestamp in future: {event_time}")
        
        if event_time < 788918400:  # Before Jan 1, 1995
            errors.append(f"Timestamp too old: {event_time}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "event": event
        }
    
    def _add_ratings_expectations(self, validator):
        """Add expectations for ratings data"""
        
        # Column existence
        validator.expect_table_columns_to_match_ordered_list(
            column_list=["user_idx", "movie_idx", "rating", "timestamp"]
        )
        
        # No nulls
        for col in ["user_idx", "movie_idx", "rating", "timestamp"]:
            validator.expect_column_values_to_not_be_null(column=col)
        
        # Rating range
        validator.expect_column_values_to_be_between(
            column="rating",
            min_value=0.5,
            max_value=5.0
        )
        
        # Valid rating values
        validator.expect_column_values_to_be_in_set(
            column="rating",
            value_set=[0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
        )
        
        # Non-negative indices
        validator.expect_column_values_to_be_between(
            column="user_idx",
            min_value=0
        )
        
        validator.expect_column_values_to_be_between(
            column="movie_idx",
            min_value=0
        )
        
        # Timestamp range
        validator.expect_column_values_to_be_between(
            column="timestamp",
            min_value=788918400,  # Jan 1, 1995
            max_value=1735689600  # Jan 1, 2025
        )
        
        # No duplicates
        validator.expect_compound_columns_to_be_unique(
            column_list=["user_idx", "movie_idx", "timestamp"]
        )
        
        # Row count
        validator.expect_table_row_count_to_be_between(
            min_value=1000,
            max_value=30000000
        )
    
    def _add_movies_expectations(self, validator):
        """Add expectations for movies data"""
        
        # Required columns
        validator.expect_table_columns_to_match_set(
            column_set=["movie_idx", "movieId", "title", "genres"]
        )
        
        # No nulls in key columns
        for col in ["movie_idx", "movieId", "title"]:
            validator.expect_column_values_to_not_be_null(column=col)
        
        # Unique indices
        validator.expect_column_values_to_be_unique(column="movie_idx")
        validator.expect_column_values_to_be_unique(column="movieId")
        
        # Non-negative indices
        validator.expect_column_values_to_be_between(
            column="movie_idx",
            min_value=0
        )
        
        # Non-empty titles
        validator.expect_column_value_lengths_to_be_between(
            column="title",
            min_value=1
        )
        
        # Row count
        validator.expect_table_row_count_to_be_between(
            min_value=100,
            max_value=100000
        )
    
    def _parse_results(self, results) -> Dict[str, Any]:
        """Parse GE validation results into clean dictionary"""
        
        success = results["success"]
        statistics = results["statistics"]
        
        failed_expectations = []
        for result in results["results"]:
            if not result["success"]:
                expectation = result["expectation_config"]
                failed_expectations.append({
                    "expectation_type": expectation["expectation_type"],
                    "column": expectation.get("kwargs", {}).get("column", "N/A"),
                    "details": result.get("result", {})
                })
        
        return {
            "valid": success,
            "evaluated_expectations": statistics["evaluated_expectations"],
            "successful_expectations": statistics["successful_expectations"],
            "failed_expectations": statistics["unsuccessful_expectations"],
            "success_rate": statistics["success_percent"],
            "failures": failed_expectations,
            "timestamp": datetime.now().isoformat()
        }
    
    def generate_report(self, results: Dict[str, Any], output_path: str = "reports/validation_report.html"):
        """Generate HTML validation report"""
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Validation Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .success {{ color: green; }}
                .failure {{ color: red; }}
                .metric {{ margin: 10px 0; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
            </style>
        </head>
        <body>
            <h1>Data Validation Report</h1>
            <p><strong>Timestamp:</strong> {results['timestamp']}</p>
            
            <div class="{'success' if results['valid'] else 'failure'}">
                <h2>Status: {'✓ PASSED' if results['valid'] else '✗ FAILED'}</h2>
            </div>
            
            <div class="metric">
                <strong>Evaluated Expectations:</strong> {results['evaluated_expectations']}
            </div>
            <div class="metric">
                <strong>Successful:</strong> {results['successful_expectations']}
            </div>
            <div class="metric">
                <strong>Failed:</strong> {results['failed_expectations']}
            </div>
            <div class="metric">
                <strong>Success Rate:</strong> {results['success_rate']:.1f}%
            </div>
            
            {self._format_failures_html(results['failures'])}
        </body>
        </html>
        """
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            f.write(html)
        
        return output_path
    
    def _format_failures_html(self, failures: List[Dict]) -> str:
        """Format failures as HTML table"""
        if not failures:
            return "<p>No failures detected.</p>"
        
        html = "<h3>Failed Expectations:</h3><table>"
        html += "<tr><th>Expectation Type</th><th>Column</th><th>Details</th></tr>"
        
        for failure in failures:
            html += f"""
            <tr>
                <td>{failure['expectation_type']}</td>
                <td>{failure['column']}</td>
                <td>{str(failure['details'])[:200]}</td>
            </tr>
            """
        
        html += "</table>"
        return html
