/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package guihua.sun.queue.codec.protocol;

import guihua.sun.queue.codec.CodecException;
import guihua.sun.queue.codec.ICodec;
import guihua.sun.queue.codec.transport.TTransport;

import java.nio.ByteBuffer;

/**
 * Protocol interface definition.
 *
 */
public abstract class TProtocol {

  /**
   * Prevent direct instantiation
   */
  @SuppressWarnings("unused")
  private TProtocol() {}

  /**
   * Transport
   */
  protected TTransport trans_;

  /**
   * Constructor
   */
  protected TProtocol(TTransport trans) {
    trans_ = trans;
  }

  /**
   * Transport accessor
   */
  public TTransport getTransport() {
    return trans_;
  }

  /**
   * Writing methods.
   */

  public abstract void writeMessageBegin(TMessage message) throws CodecException;

  public abstract void writeMessageEnd() throws CodecException;

  public abstract void writeStructBegin(TStruct struct) throws CodecException;

  public abstract void writeStructEnd() throws CodecException;

  public abstract void writeFieldBegin(TField field) throws CodecException;

  public abstract void writeFieldEnd() throws CodecException;

  public abstract void writeFieldStop() throws CodecException;

  public abstract void writeMapBegin(TMap map) throws CodecException;

  public abstract void writeMapEnd() throws CodecException;

  public abstract void writeListBegin(TList list) throws CodecException;

  public abstract void writeListEnd() throws CodecException;

  public abstract void writeSetBegin(TSet set) throws CodecException;

  public abstract void writeSetEnd() throws CodecException;

  public abstract void writeBool(boolean b) throws CodecException;

  public abstract void writeByte(byte b) throws CodecException;

  public abstract void writeI16(short i16) throws CodecException;

  public abstract void writeI32(int i32) throws CodecException;

  public abstract void writeI64(long i64) throws CodecException;

  public abstract void writeDouble(double dub) throws CodecException;

  public abstract void writeString(String str) throws CodecException;

  public abstract void writeBinary(ByteBuffer buf) throws CodecException;

  /**
   * Reading methods.
   */

  public abstract TMessage readMessageBegin() throws CodecException;

  public abstract void readMessageEnd() throws CodecException;

  public abstract TStruct readStructBegin() throws CodecException;

  public abstract void readStructEnd() throws CodecException;

  public abstract TField readFieldBegin() throws CodecException;

  public abstract void readFieldEnd() throws CodecException;

  public abstract TMap readMapBegin() throws CodecException;

  public abstract void readMapEnd() throws CodecException;

  public abstract TList readListBegin() throws CodecException;

  public abstract void readListEnd() throws CodecException;

  public abstract TSet readSetBegin() throws CodecException;

  public abstract void readSetEnd() throws CodecException;

  public abstract boolean readBool() throws CodecException;

  public abstract byte readByte() throws CodecException;

  public abstract short readI16() throws CodecException;

  public abstract int readI32() throws CodecException;

  public abstract long readI64() throws CodecException;

  public abstract double readDouble() throws CodecException;

  public abstract String readString() throws CodecException;

  public abstract ByteBuffer readBinary() throws CodecException;

  /**
   * Reset any internal state back to a blank slate. This method only needs to
   * be implemented for stateful protocols.
   */
  public void reset() {}
  
}
