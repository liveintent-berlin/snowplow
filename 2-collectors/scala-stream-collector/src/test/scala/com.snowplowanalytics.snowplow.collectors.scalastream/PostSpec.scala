/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package collectors
package scalastream

// Scala
import org.specs2.mock.Mockito
import spray.http.HttpHeaders.RawHeader
import spray.http.{StatusCodes, Uri}

import scala.collection.mutable.MutableList

// Akka
import akka.actor.{ActorSystem, Props}

// Specs2 and Spray testing
import org.specs2.matcher.AnyMatchers
import org.specs2.mutable.Specification
import org.specs2.specification.{Scope,Fragments}
import spray.testkit.Specs2RouteTest

// Spray
import spray.http.{DateTime, HttpHeader, HttpRequest, HttpCookie, RemoteAddress}
import spray.http.HttpHeaders.{
  Cookie,
  `Set-Cookie`,
  `Remote-Address`,
  `Raw-Request-URI`
}

// Config
import com.typesafe.config.{ConfigFactory,Config,ConfigException}

// Thrift
import org.apache.thrift.TDeserializer

// Snowplow
import sinks._
import CollectorPayload.thrift.model1.CollectorPayload

class PostSpec extends Specification with Specs2RouteTest with
     AnyMatchers with Mockito {
   def testConf(n3pcEnabled: Boolean = false): Config = ConfigFactory.parseString(s"""
collector {
  interface = "0.0.0.0"
  port = 8080

  production = true
  third-party-redirect-enabled = $n3pcEnabled
  p3p {
    policyref = "/w3c/p3p.xml"
    CP = "NOI DSP COR NID PSA OUR IND COM NAV STA"
  }

  cookie {
    enabled = true
    expiration = 365 days
    name = sp
    domain = "test-domain.com"
  }

  sink {
    enabled = "test"

    kinesis {
      aws {
        access-key: "cpf"
        secret-key: "cpf"
      }
      stream {
        region: "us-east-1"
        good: "snowplow_collector_example"
        bad: "snowplow_collector_example"
      }
      buffer {
        byte-limit: 4000000 # 4MB
        record-limit: 500 # 500 records
        time-limit: 60000 # 1 minute
      }
      backoffPolicy {
        minBackoff: 3000 # 3 seconds
        maxBackoff: 600000 # 5 minutes
      }
    }
  }
}
""")
  // By default, spray will always add Remote-Address to every request
  // when running with the `spray.can.server.remote-address-header`
  // option. However, the testing does not read this option and a
  // remote address always needs to be set.
  def CollectorPost(uri: String, cookie: Option[`HttpCookie`] = None,
                    remoteAddr: String = "127.0.0.1") = {
    val headers: MutableList[HttpHeader] =
      MutableList(`Remote-Address`(remoteAddr),`Raw-Request-URI`(uri))
    cookie.foreach(headers += `Cookie`(_))
    Post(uri).withHeaders(headers.toList)
  }

  "Snowplow's Scala collector without n3pc redirects" should {
    val collectorConfig = new CollectorConfig(testConf())
    val sink = new TestSink
    val sinks = CollectorSinks(sink, sink)
    val responseHandler = new ResponseHandler(collectorConfig, sinks)
    val collectorService = new CollectorService(collectorConfig, responseHandler, system)
    val thriftDeserializer = new TDeserializer

    "return a cookie expiring at the correct time" in {
      CollectorPost("/com.snowplowanalytics.snowplow/tp2") ~> collectorService.collectorRoute ~> check {
        headers must not be empty

        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        httpCookies must not be empty

        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.path must beSome("/")
        httpCookie.name must beEqualTo(collectorConfig.cookieName.get)
        httpCookie.name must beEqualTo("sp")
        httpCookie.path must beSome("/")
        httpCookie.domain must beSome
        httpCookie.domain.get must be(collectorConfig.cookieDomain.get)
        httpCookie.expires must beSome
        httpCookie.content.matches("""[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}""")
        val expiration = httpCookie.expires.get
        val offset = expiration.clicks - collectorConfig.cookieExpiration.get - DateTime.now.clicks
        offset.asInstanceOf[Int] must beCloseTo(0, 2000) // 1000 ms window.
      }
    }
    "return a cookie containing nuid query parameter" in {
      CollectorPost("/com.snowplowanalytics.snowplow/tp2?nuid=UUID_Test_New") ~> collectorService.collectorRoute ~> check {
        headers must not be empty

        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        httpCookies must not be empty

        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.name must beEqualTo(collectorConfig.cookieName.get)
        httpCookie.name must beEqualTo("sp")
        httpCookie.path must beSome("/")
        httpCookie.domain must beSome
        httpCookie.domain.get must be(collectorConfig.cookieDomain.get)
        httpCookie.expires must beSome
        httpCookie.content must beEqualTo("UUID_Test_New")
        val expiration = httpCookie.expires.get
        val offset = expiration.clicks - collectorConfig.cookieExpiration.get - DateTime.now.clicks
        offset.asInstanceOf[Int] must beCloseTo(0, 3600000) // 1 hour window.
      }
    }
    "return the same cookie as passed in" in {
      CollectorPost("/com.snowplowanalytics.snowplow/tp2", Some(HttpCookie(collectorConfig.cookieName.get, "UUID_Test"))) ~>
          collectorService.collectorRoute ~> check {
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.content must beEqualTo("UUID_Test")
      }
    }
    "override cookie with nuid parameter" in {
      CollectorPost("/com.snowplowanalytics.snowplow/tp2?nuid=UUID_Test_New", Some(HttpCookie("sp", "UUID_Test"))) ~>
          collectorService.collectorRoute ~> check {
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.content must beEqualTo("UUID_Test_New")
      }
    }
    "return a P3P header" in {
      CollectorPost("/com.snowplowanalytics.snowplow/tp2") ~> collectorService.collectorRoute ~> check {
        val p3pHeaders = headers.filter {
          h => h.name.equals("P3P")
        }
        p3pHeaders.size must beEqualTo(1)
        val p3pHeader = p3pHeaders(0)

        val policyRef = collectorConfig.p3pPolicyRef
        val CP = collectorConfig.p3pCP
        p3pHeader.value must beEqualTo(
          "policyref=\"%s\", CP=\"%s\"".format(policyRef, CP))
      }
    }
    "store the expected event as a serialized Thrift object in the enabled sink" in {
      val payloadData = "param1=val1&param2=val2"
      val storedRecordBytes = responseHandler.cookie(payloadData, null, None,
        None, "localhost", RemoteAddress("127.0.0.1"), new HttpRequest(), None, "/com.snowplowanalytics.snowplow/tp2", false)._2

      val storedEvent = new CollectorPayload
      this.synchronized {
        thriftDeserializer.deserialize(storedEvent, storedRecordBytes.head)
      }

      storedEvent.timestamp must beCloseTo(DateTime.now.clicks, 60000)
      storedEvent.encoding must beEqualTo("UTF-8")
      storedEvent.ipAddress must beEqualTo("127.0.0.1")
      storedEvent.collector must beEqualTo("ssc-0.8.0-test")
      storedEvent.path must beEqualTo("/com.snowplowanalytics.snowplow/tp2")
      storedEvent.querystring must beEqualTo(payloadData)
    }
  }

  "Snowplow's Scala collector with n3pc redirects" should {
    val collectorConfig = new CollectorConfig(testConf(n3pcEnabled = true))
    val sink = new TestSink
    val sinks = CollectorSinks(sink, sink)
    val responseHandler = new ResponseHandler(collectorConfig, sinks)
    val collectorService = new CollectorService(collectorConfig, responseHandler, system)
    val thriftDeserializer = new TDeserializer

    val path = "/com.snowplowanalytics.snowplow/tp2"
    val hostAndPath = collectorConfig.interface + path

    "return a cookie expiring at the correct time" in {
      CollectorPost(path) ~> collectorService.collectorRoute ~> check {
        headers must not be empty

        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        httpCookies must not be empty

        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.path must beSome("/")
        httpCookie.name must beEqualTo(collectorConfig.cookieName.get)
        httpCookie.name must beEqualTo("sp")
        httpCookie.path must beSome("/")
        httpCookie.domain must beSome
        httpCookie.domain.get must be(collectorConfig.cookieDomain.get)
        httpCookie.expires must beSome
        httpCookie.content.matches("""[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}""")
        val expiration = httpCookie.expires.get
        val offset = expiration.clicks - collectorConfig.cookieExpiration.get - DateTime.now.clicks
        offset.asInstanceOf[Int] must beCloseTo(0, 2000) // 1000 ms window.
      }
    }

    "return a cookie containing nuid query parameter" in {
      CollectorPost(s"$path?nuid=UUID_Test_New") ~> collectorService.collectorRoute ~> check {
        headers must not be empty

        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        httpCookies must not be empty

        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.name must beEqualTo(collectorConfig.cookieName.get)
        httpCookie.name must beEqualTo("sp")
        httpCookie.path must beSome("/")
        httpCookie.domain must beSome
        httpCookie.domain.get must be(collectorConfig.cookieDomain.get)
        httpCookie.expires must beSome
        httpCookie.content must beEqualTo("UUID_Test_New")
        val expiration = httpCookie.expires.get
        val offset = expiration.clicks - collectorConfig.cookieExpiration.get - DateTime.now.clicks
        offset.asInstanceOf[Int] must beCloseTo(0, 3600000) // 1 hour window.
      }
    }

    "return the same cookie as passed in" in {
      CollectorPost(path, Some(HttpCookie(collectorConfig.cookieName.get, "UUID_Test"))) ~>
        collectorService.collectorRoute ~> check {
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.content must beEqualTo("UUID_Test")
      }
    }

    "override cookie with nuid parameter" in {
      CollectorPost(s"$path?nuid=UUID_Test_New", Some(HttpCookie("sp", "UUID_Test"))) ~>
        collectorService.collectorRoute ~> check {
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.content must beEqualTo("UUID_Test_New")
      }
    }

    "return a P3P header" in {
      CollectorPost(path) ~> collectorService.collectorRoute ~> check {
        val p3pHeaders = headers.filter {
          h => h.name.equals("P3P")
        }
        p3pHeaders.size must beEqualTo(1)
        val p3pHeader = p3pHeaders(0)

        val policyRef = collectorConfig.p3pPolicyRef
        val CP = collectorConfig.p3pCP
        p3pHeader.value must beEqualTo(
          "policyref=\"%s\", CP=\"%s\"".format(policyRef, CP))
      }
    }

    "return a redirect with n3pc parameter" in {

      val sinkMock = smartMock[AbstractSink]
      val sinkStubs = CollectorSinks(sinkMock, sinkMock)
      val responseHandlerStub = new ResponseHandler(collectorConfig, sinkStubs)
      val collectorServiceStub = new CollectorService(collectorConfig, responseHandlerStub, system)

      CollectorPost(path) ~> collectorServiceStub.collectorRoute ~> check {
        headers must not be empty

        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        httpCookies must not be empty

        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.path must beSome("/")
        httpCookie.name must beEqualTo(collectorConfig.cookieName.get)
        httpCookie.path must beSome("/")
        httpCookie.domain must beSome
        httpCookie.domain.get must be(collectorConfig.cookieDomain.get)
        httpCookie.expires must beSome
        httpCookie.content.matches("""[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}""")
        val expiration = httpCookie.expires.get
        val offset = expiration.clicks - collectorConfig.cookieExpiration.get - DateTime.now.clicks
        offset.asInstanceOf[Int] must beCloseTo(0, 2000) // 1000 ms window.
        response.status.mustEqual(StatusCodes.TemporaryRedirect)
        //we're redirecting so we should not save anything to kinesis
        there was no(sinkMock).storeRawEvents(any, any)
        val locationHeader: String = header("Location").get.value
        println(locationHeader)
        locationHeader must beEqualTo(s"http://example.com$path?${collectorConfig.thirdPartyCookiesParameter}=true")
        locationHeader must contain(collectorConfig.thirdPartyCookiesParameter)
        httpCookies must not be empty
      }
    }

    "set the fallback cookie value after calling it with n3pc parameter without cookies" in {
      val sinkMock = smartMock[AbstractSink]
      sinkMock.storeRawEvents(any[List[Array[Byte]]], anyString).answers { (params, mock) =>
        params match {
          case Array(firstArg, secondArg) => firstArg.asInstanceOf[List[Array[Byte]]]
        }
      }
      val sinkStubs = CollectorSinks(sinkMock, sinkMock)
      val responseHandlerStub = new ResponseHandler(collectorConfig, sinkStubs)
      val collectorServiceStub = new CollectorService(collectorConfig, responseHandlerStub, system)

      CollectorPost(path + s"?${collectorConfig.thirdPartyCookiesParameter}=true") ~> collectorServiceStub.collectorRoute ~> check {
        headers must not be empty

        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        httpCookies must not be empty

        // Assume we only return a single cookie.
        // If the collector is modified to return multiple cookies,
        // this will need to be changed.
        val httpCookie = httpCookies(0)

        httpCookie.path must beSome("/")
        httpCookie.name must beEqualTo(collectorConfig.cookieName.get)
        httpCookie.path must beSome("/")
        httpCookie.domain must beSome(collectorConfig.cookieDomain.get)
        httpCookie.expires must beSome
        httpCookie.content mustEqual collectorConfig.fallbackNetworkUserId
      }
    }

    "return the correct cookie even after calling it with n3pc parameter" in {

      val sinkMock = smartMock[AbstractSink]
      sinkMock.storeRawEvents(any[List[Array[Byte]]], anyString).answers { (params, mock) =>
        params match {
          case Array(firstArg, secondArg) => firstArg.asInstanceOf[List[Array[Byte]]]
        }
      }
      val sinkStubs = CollectorSinks(sinkMock, sinkMock)
      val responseHandlerStub = new ResponseHandler(collectorConfig, sinkStubs)
      val collectorServiceStub = new CollectorService(collectorConfig, responseHandlerStub, system)

      CollectorPost(path + s"?${collectorConfig.thirdPartyCookiesParameter}=true", cookie = Some(HttpCookie(collectorConfig.cookieName.get, "UUID_Test"))) ~> collectorServiceStub.collectorRoute ~> check {
        val httpCookies: List[HttpCookie] = headers.collect {
          case `Set-Cookie`(hc) => hc
        }
        there was two(sinkMock).storeRawEvents(any, any)
        val httpCookie = httpCookies.head
        httpCookie.content must beEqualTo("UUID_Test")
      }
    }


    "store the expected event as a serialized Thrift object in the enabled sink after the cookie bounce" in {
      val payloadData = "param1=val1&param2=val2"

      val request = HttpRequest(uri = Uri(hostAndPath).withQuery(collectorConfig.thirdPartyCookiesParameter -> "true"))
      val storedRecordBytes = responseHandler.cookie(payloadData, null, None,
        None, "localhost", RemoteAddress("127.0.0.1"), request, None, path, false)._2

      val storedEvent = new CollectorPayload
      this.synchronized {
        thriftDeserializer.deserialize(storedEvent, storedRecordBytes.head)
      }

      storedEvent.timestamp must beCloseTo(DateTime.now.clicks, 60000)
      storedEvent.encoding must beEqualTo("UTF-8")
      storedEvent.ipAddress must beEqualTo("127.0.0.1")
      storedEvent.collector must beEqualTo("ssc-0.8.0-test")
      storedEvent.path must beEqualTo(path)
      storedEvent.querystring must beEqualTo(payloadData)
    }
  }
}
