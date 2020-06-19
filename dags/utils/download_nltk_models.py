import nltk

def download_nltk_data(*args, **kwargs):
    for data_file in ['punkt', 'averaged_perceptron_tagger', 'wordnet', 'universal_tagset', 'vader_lexicon']:
        nltk.download(data_file)
