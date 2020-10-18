import twint

c = twint.Config()
c.Username = "@RanaDaggubati"
c.Database = "users.db"
c.User_full = True

twint.run.Followers(c)
