package com.spark.project.web

import com.spark.project.common.Config
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer

import scala.concurrent.ExecutionContextExecutor

/**
 * Web 服务器入口
 *
 * 提供:
 * - 静态资源服务（HTML/CSS/JS）
 * - RESTful API 接口（数据查询、分析结果获取）
 * - 模板渲染（Thymeleaf 动态页面）
 *
 * 部署后可通过 http://<服务器IP>:8080 远程访问
 *
 * TODO: 实现完整的 Web 路由和 API
 */
object WebServer {

  def main(args: Array[String]): Unit = {
    // TODO: 实现 Web 服务器启动逻辑
    // 1. 创建 ActorSystem
    // 2. 注册 API 路由（调用 ApiRoutes）
    // 3. 配置静态资源目录
    // 4. 绑定到 Config.WEB_HOST:Config.WEB_PORT
    // 5. 启动服务器
    // 6. 打印访问地址
    ???
  }

  /**
   * 启动 Web 服务器（可被 Main 调用）
   */
  def start(): Unit = {
    main(Array.empty)
  }
}
