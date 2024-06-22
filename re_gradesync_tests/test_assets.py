from re_gradesync.assets import m365_users, m365_assignments, m365_submissions

def test_m365_users():
    df_users = m365_users()
    assert df_users is not None

def test_m365_assignments():
    df_assignments = m365_assignments()
    assert df_assignments is not None

def test_m365_submissions():
    df_submissions = m365_submissions()
    assert df_submissions is not None
