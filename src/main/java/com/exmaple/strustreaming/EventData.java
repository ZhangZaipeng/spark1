package com.exmaple.strustreaming;

import java.sql.Timestamp;

public class EventData {

  //timestamp: Timestamp,
  //word: String

  public EventData() {
  }

  public EventData(Timestamp wordtime, String word) {
    this.wordtime = wordtime;
    this.word = word;
  }

  private Timestamp wordtime;
  private String word;

  public Timestamp getWordtime() {
    return wordtime;
  }

  public void setWordtime(Timestamp wordtime) {
    this.wordtime = wordtime;
  }

  public String getWord() {
    return word;
  }

  public void setWord(String word) {
    this.word = word;
  }
}
