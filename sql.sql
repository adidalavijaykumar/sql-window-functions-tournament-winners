CREATE TABLE Players (player_id INT,
group_id INT
)

INSERT INTO Players (player_id, group_id)
VALUES (15,1),(25,1),(30,1),(45,1),(10,1),(35,1),(50,2),(20,3),(40,3);

select *from Players


CREATE TABLE Matches (
match_id INT PRIMARY KEY,
first_player INT,
second_player INT,
first_score INT,
second_score INT
)
SELECT * FROM Matches
INSERT INTO Matches (match_id, first_player, second_player, first_score, second_score)
VALUES (1, 15, 45, 3, 0),
    (2, 30, 25, 1, 2),
    (3, 30, 15, 2, 0),
    (4, 40, 20, 5, 2),
    (5, 35, 50, 1, 1);
---converting 5 columns into 3 columns
with cte as (SELECT match_id, 
first_player as player,
first_score as score
from Matches
union all
SELECT match_id,
second_player as player,
second_score as score
FROM Matches),
cte_joined as
(SELECT c.*, p.group_id
FROM cte c
JOIN Players p
on c.player = p.player_id);


with cte as (SELECT match_id, 
first_player as player,
first_score as score
from Matches
union all
SELECT match_id,
second_player as player,
second_score as score
FROM Matches),
cte_joined as
(SELECT c.*, p.group_id
FROM cte c
JOIN Players p
on c.player = p.player_id),
cte_grouped as (SELECT group_id, player, sum(score) as total_score
FROM cte_joined
group by group_id, player)


with cte as (SELECT match_id, 
first_player as player,
first_score as score
from Matches
union all
SELECT match_id,
second_player as player,
second_score as score
FROM Matches),
cte_joined as
(SELECT c.*, p.group_id
FROM cte c
JOIN Players p
on c.player = p.player_id),
cte_grouped as (SELECT group_id, player, sum(score) as total_score
FROM cte_joined
group by group_id, player),
cte_windowed as 
(SELECT group_id, player, 
rank() over (partition by group_id order by total_score desc, player)
as rnk FROM cte_grouped)
SELECT group_id, player as player_id
from cte_windowed WHERE rnk = 1

--APPROACH 2

SELECT p.*, m.*
FROM Players p
JOIN Matches m
ON p.player_id in (m.first_player, m.second_player)

SELECT p.group_id,p.player_id,
case when p.player_id = m.first_player then m.first_score
else m.second_score
end as total_score
FROM Players p
JOIN Matches m
ON p.player_id in (m.first_player, m.second_player)

SELECT p.group_id,p.player_id,
SUM(case when p.player_id = m.first_player then m.first_score
else m.second_score
end) as total_score,
rank() over (partition by p.group_id order by
    sum(case when p.player_id = m.first_player then m.first_score
else m.second_score
end) desc,player_id) as rnk
FROM Players p
JOIN Matches m
ON p.player_id in (m.first_player, m.second_player)
group by p.group_id, p.player_id

select group_id,player_id from
(SELECT group_id,player_id, total_score,
rank() over (partition by group_id order by total_score desc, player_id) as rnk 
from
(select p.group_id, p.player_id,                                 n  mm  m mcm
sum(case when p.player_id = m.first_player then m.first_score
else m.second_score
end) as total_score
FROM Players p
JOIN Matches m
ON p.player_id in (m.first_player, m.second_player)
group by p.group_id, p.player_id) c ) final
where rnk = 1;

