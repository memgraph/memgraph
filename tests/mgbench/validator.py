import mgclient

if __name__ == "__main__":

    conn = mgclient.connect(host="127.0.0.1", port=7687)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE (n:Person {name: 'John'})-[e:KNOWS]->
               (m:Person {name: 'Steve'})
        RETURN n, e, m
    """
    )

    row = cursor.fetchone()
    print(row[0])

    conn.commit()

    cursor.execute(
        """
        MATCH (n) RETURN n;
        """
    )
    row = cursor.fetchall()
    print(row)
