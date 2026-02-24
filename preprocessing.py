from transformers import pipeline

def process_data(comments, submissions):
    pipe_finbert = pipeline(
        "text-classification",
        model="/opt/models/finbert",
        tokenizer="/opt/models/finbert",
        local_files_only=True,
        device=-1,  # Lambda is usually CPU unless you’re on a GPU service
    )

    pipe_twitter = pipeline(
        "text-classification",
        model="/opt/models/twitter_roberta_sentiment",
        tokenizer="/opt/models/twitter_roberta_sentiment",
        local_files_only=True,
        device=-1,
        batch_size=64,
    )

