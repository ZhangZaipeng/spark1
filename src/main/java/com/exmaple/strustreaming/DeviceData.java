package com.exmaple.strustreaming;

import java.sql.Date;

public class DeviceData {

  //device: string, type: string, signal: double, time: DateType
  private String device;
  private String deviceType;
  private double signal;
  private Date deviceTime;

  public DeviceData() {
  }

  public DeviceData(String device, String deviceType, double signal, Date deviceTime) {
    this.device = device;
    this.deviceType = deviceType;
    this.signal = signal;
    this.deviceTime = deviceTime;
  }

  public String getDevice() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public String getDeviceType() {
    return deviceType;
  }

  public void setDeviceType(String deviceType) {
    this.deviceType = deviceType;
  }

  public double getSignal() {
    return signal;
  }

  public void setSignal(double signal) {
    this.signal = signal;
  }

  public Date getDeviceTime() {
    return deviceTime;
  }

  public void setDeviceTime(Date deviceTime) {
    this.deviceTime = deviceTime;
  }
}
