/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import com.googlecode.javaewah.EWAHCompressedBitmap

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/*
  * A function that return the bit wise AND operation between two Ewah Bitmaps
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1, bitmap2) - Returns the bitwise AND",
  extended = "> SELECT _FUNC_('13,2,4,8589934592,4096,0', '13,2,4,8589934592,4096,0');")
case class EwahBitmapAnd(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback {

  override def dataType: DataType = left.dataType

  override def nullSafeEval(bitmap_1: Any, bitmap_2: Any): Any = {

    val b1: EWAHCompressedBitmap = new EWAHCompressedBitmap
    val b2: EWAHCompressedBitmap = new EWAHCompressedBitmap

    val bitmap1_String = bitmap_1.asInstanceOf[UTF8String].toString.split(",")
    val bitmap2_String = bitmap_2.asInstanceOf[UTF8String].toString.split(",")

    var a = 0;
    for (a <- 0 until bitmap1_String.length) {
      if (a == bitmap1_String.length - 1) {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), true)
      }
      else {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), false)
      }
    }

    var b = 0;
    for (b <- 0 until bitmap2_String.length) {
      if (b == bitmap2_String.length - 1) {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), true)
      }
      else {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), false)
      }
    }

    val result: EWAHCompressedBitmap = b1.and(b2)
    val utf8String: UTF8String = UTF8String.fromString(result.toRLWString)
    return utf8String
  }
}


/*
  * A function that return the bit wise OR operation between two Ewah Bitmaps
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1, bitmap2) - Returns the bitwise OR",
  extended = "> SELECT _FUNC_('13,2,4,8589934592,4096,0', '13,2,4,8589934592,4096,0');")
case class EwahBitmapOr(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback {

  override def dataType: DataType = left.dataType

  override def nullSafeEval(bitmap_1: Any, bitmap_2: Any): Any = {


    val b1: EWAHCompressedBitmap = new EWAHCompressedBitmap
    val b2: EWAHCompressedBitmap = new EWAHCompressedBitmap

    val bitmap1_String = bitmap_1.asInstanceOf[UTF8String].toString.split(",")
    val bitmap2_String = bitmap_2.asInstanceOf[UTF8String].toString.split(",")

    var a = 0;
    for (a <- 0 until bitmap1_String.length) {
      if (a == bitmap1_String.length - 1) {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), true)
      }
      else {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), false)
      }
    }

    var b = 0;
    for (b <- 0 until bitmap2_String.length) {
      if (b == bitmap2_String.length - 1) {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), true)
      }
      else {
        b2.setBufferWord(b, java.lang.Long.parseLong(bitmap2_String(b)), false)
      }
    }
    val result: EWAHCompressedBitmap = b1.or(b2)
    val utf8String: UTF8String = UTF8String.fromString(result.toRLWString)
    return utf8String
  }
}

/*
  * A function that return the count of bits set to true in the given Ewah Bitmap
  */
@ExpressionDescription(
  usage = "_FUNC_(bitmap1) - Returns the count of bits set to true",
  extended = "> SELECT _FUNC_('13,2,4,8589934592,4096,0');")
case class EwahBitmapCount(child: Expression)
  extends UnaryExpression with CodegenFallback {

  override def dataType: DataType = IntegerType

  override def nullSafeEval(bitmap_1: Any): Any = {

    val b1: EWAHCompressedBitmap = new EWAHCompressedBitmap

    val bitmap1_String = bitmap_1.asInstanceOf[UTF8String].toString.split(",")

    var a = 0;
    for (a <- 0 until bitmap1_String.length) {
      if (a == bitmap1_String.length - 1) {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), true)
      }
      else {
        b1.setBufferWord(a, java.lang.Long.parseLong(bitmap1_String(a)), false)
      }
    }
    return b1.cardinality()
  }
}