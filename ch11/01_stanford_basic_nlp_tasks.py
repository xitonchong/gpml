import stanfordnlp




class BasicNLP(object): 
    def __init__(self, language): 
        stanfordnlp.download(download_label=language, resource_dir='stanfordnlp_resource', confirm_if_exists=False)

    def tokenize(self, text): 
        nlp = stanfordnlp.Pipeline(models_dir="stanfordnlp_resources") # this sets up a default neural pipeline in english
        # Todo: investigate why it fails here 
        doc = nlp(text)



if __name__ == '__main__':
    basic_nlp = BasicNLP()
    basic_nlp.tokenize('barack obama was born in Hawaii. He was elected president in 2008.')