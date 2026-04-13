import pickle

class TimerPerf:
    def __init__(self) -> None:
        pass

    def __call__(self,path,detail=False):

        with open(f'{path}/data',"rb") as f:
            timer_data = pickle.load(f)

        results = {}
        if detail:
            for key, data in timer_data.timings.items():
                sums = []
                for line,hit,secs in data:
                    sums.append(secs)
                    
                    print('%f' % (timer_data.unit*sum(sums)))


                    