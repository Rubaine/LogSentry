import pandas as pd

sample_log = [
    '185.224.128.83 - - [29/Sep/2024:00:24:27 +0200] "GET /cgi-bin/luci/;stok=/locale?form=country&operation=write&country=id%3E%60for+pid+in+%2Fproc%2F%5B0-9%5D%2A%2F%3B+do+pid%3D%24%7Bpid%25%2F%7D%3B+pid%3D%24%7Bpid%23%23%2A%2F%7D%3B+exe_path%3D%24%28ls+-l+%2Fproc%2F%24pid%2Fexe+2%3E%2Fdev%2Fnull+%7C+awk+%27%7Bprint+%24NF%7D%27%29%3B+if+%5B%5B+%24exe_path+%3D%3D+%2A%2F+%5D%5D%3B+then+kill+-9+%24pid%3B+fi%3B+done%3B%60 HTTP/1.1" 404 162 "-" "Go-http-client/1.1"'
]

def parse_log_file(log_file_path, output_csv_path):
    requests = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE"]
    ip = []
    datetime = []
    request = []
    url = []
    status_code = []
    user_agent = []

    
    for line in log_file_path:
        parts = line.split('"')
        print(parts)
        if len(parts) > 1:
            log_part = parts[1].split(' ')
            if len(log_part) > 1:
                # Si la première partie est une requête, on la capture comme requête et l'URL comme deuxième partie
                if log_part[0] in requests:
                    request.append(log_part[0])
                    url.append(' '.join(log_part[1:-1]))  # L'URL sans la dernière partie (HTTP version)
                else:
                    # Sinon, il s'agit d'une ligne malformée où il n'y a pas de requête
                    request.append(None)
                    url.append(' '.join(log_part))  # Traiter toute la ligne comme une URL potentielle
            else:
                # Si la ligne ne contient pas plusieurs parties (ex. : requête manquante), traiter comme malformée
                request.append(None)
                url.append(None)
        else:
            # Si la ligne ne contient même pas de guillemets (malformée), aucune URL ni requête ne sont extraites
            request.append(None)
            url.append(None)
        
        print(request)
        print(url)  


parse_log_file(sample_log, 'dataframes/logs.csv')
