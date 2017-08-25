// Message Content
MATCH (m:Message {id:"1236950581249"})
RETURN
  CASE exists(m.content)
    WHEN true THEN m.content
    ELSE m.imageFile
  END AS messageContent,
  m.creationDate as messageCreationDate;
