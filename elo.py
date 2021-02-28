
def prob_t1_win(rating1, rating2):
    return 1 / (1 + pow(10, ((rating2 - rating1) / 400)))

def prob_t1_t2_win(rating1, rating2):
    prob_t1 = prob_t1_win(rating1, rating2)
    return prob_t1, 1- prob_t1


def avg_team_elo(players):
    return sum([p.elo for p in players]) / len(players)

def avg_team_elo_dict(team):
    return sum([p['elo'] for p in team]) / len(team)

K = 30


def team_win_elos(team1, team2):
    r1 = avg_team_elo(team1)
    r2 = avg_team_elo(team2)

    prob_t1, prob_t2 = prob_t1_t2_win(r1, r2)
    team1_win_elo_inc = K * (1 - prob_t1)
    team2_win_elo_inc = K - team1_win_elo_inc
    return team1_win_elo_inc, team2_win_elo_inc


def team_win_elos_dict(team1_avg_elo, team2_avg_elo):
    r1 = team1_avg_elo
    r2 = team2_avg_elo

    prob_t1, prob_t2 = prob_t1_t2_win(r1, r2)
    team1_win_elo_inc = K * (1 - prob_t1)
    team2_win_elo_inc = K - team1_win_elo_inc
    return team1_win_elo_inc, team2_win_elo_inc


def teams_update_elo(team1, team2, winner):
    team1_win_elo_inc, team2_win_elo_inc = team_win_elos(team1, team2)

    if winner == 1 or winner == 'sentinel':
        team1[0].elo += 3   # 1.1 extra elo for winning captains
        for player in team1:
            player.elo += team1_win_elo_inc
        for player in team2:
            player.elo += -team1_win_elo_inc
        return team1_win_elo_inc, -team1_win_elo_inc
    elif winner == 2 or winner == 'scourge':
        team2[0].elo += 2
        for player in team1:
            player.elo += -team2_win_elo_inc
        for player in team2:
            player.elo += team2_win_elo_inc
        return -team2_win_elo_inc, team2_win_elo_inc
    else:
        raise Exception('WhoWonException')


if __name__ == '__main__':

    class Player:
        def __init__(self, elo):
            self.elo = elo


    team1 = [Player(elo) for elo in [1000, 2000, 0, 1000, 1000]]
    team2 = [Player(elo) for elo in [1000, 1300, 1000, 1000, 1000]]

    r1 = avg_team_elo(team1)
    r2 = avg_team_elo(team2)

    prob_t1, prob_t2 = prob_t1_t2_win(r1, r2)

    print([player.elo for player in team1])
    print([player.elo for player in team2])

    print(prob_t1, prob_t2)

    team1_win_elo_inc, team2_win_elo_inc = team_win_elos(team1, team2)

    if False:
        for player in team1:
            player.elo += team1_win_elo_inc
        for player in team2:
            player.elo += -team1_win_elo_inc
    else:
        for player in team1:
            player.elo += -team2_win_elo_inc
        for player in team2:
            player.elo += team2_win_elo_inc

    print('team1 K:', K * (1 - prob_t1), K * (-prob_t1))
    print('team2 K:', K * (1 - prob_t2), K * (-prob_t2))
    print(r1, r2)
    print([player.elo for player in team1])
    print([player.elo for player in team2])


