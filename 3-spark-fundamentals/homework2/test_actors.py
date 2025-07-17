from chispa.dataframe_comparer import *
from ..jobs.actors import do_game_details_edge_transformation
from collections import namedtuple

GameDetail = namedtuple("GameDetail", "player_id game_id start_position pts team_id team_abbreviation")
GameEdge = namedtuple("GameEdge", "subject_identifier subject_type object_identifier object_type edge_type properties")


def test_game_details_edge_transformation(spark):
    # Sample data for game_details
    source_data = [
        GameDetail(1, 101, 'G', 25, 123, 'BOS'),
        GameDetail(2, 101, 'F', 15, 456, 'LAL'),
        GameDetail(1, 102, 'G', 30, 123, 'BOS'),  # Duplicate player-game
        GameDetail(1, 102, 'F', 31, 123, 'BOS'),  # Duplicate, higher pts, should be chosen
    ]
    source_df = spark.createDataFrame(source_data)
    actual_df = do_game_details_edge_transformation(spark, source_df)

    expected_output = [
        GameEdge(
            subject_identifier=1,
            subject_type='player',
            object_identifier=101,
            object_type='game',
            edge_type='plays_in',
            properties={'start_position': 'G', 'pts': '25', 'team_id': '123', 'team_abbreviation': 'BOS'}),
        GameEdge(
            subject_identifier=2,
            subject_type='player',
            object_identifier=101,
            object_type='game',
            edge_type='plays_in',
            properties={'start_position': 'F', 'pts': '15', 'team_id': '456', 'team_abbreviation': 'LAL'}),
        GameEdge(
            subject_identifier=1,
            subject_type='player',
            object_identifier=102,
            object_type='game',
            edge_type='plays_in',
            properties={'start_position': 'F', 'pts': '31', 'team_id': '123', 'team_abbreviation': 'BOS'}),
    ]
    expected_df = spark.createDataFrame(expected_output)

    # Sort both dataframes by a key to ensure order before comparison
    key_cols = ["subject_identifier", "object_identifier"]
    assert_df_equality(actual_df.sort(key_cols), expected_df.sort(key_cols), ignore_row_order=False, ignore_nullable=True)