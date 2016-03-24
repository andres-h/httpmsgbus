SET(HTTPMSGBUS_TARGET httpmsgbus)

SET(
    HTTPMSGBUS_SOURCES
    bsonio.go
    bus.go
    filedb.go
    filter.go
    jsonio.go
    main.go
    message.pb.go
    mongodb.go
    producer.go
    queue.go
    session.go
    types.go
    utils.go
)

ADD_EXECUTABLE(${HTTPMSGBUS_TARGET} ${HTTPMSGBUS_SOURCES})

ADD_CUSTOM_TARGET(
    HTTPMSGBUS_VENDOR
    COMMAND cp -r ${CMAKE_CURRENT_SOURCE_DIR}/../vendor .
)

ADD_DEPENDENCIES(${HTTPMSGBUS_TARGET} HMBDEPS HTTPMSGBUS_VENDOR)

INSTALL(TARGETS ${HTTPMSGBUS_TARGET}
    DESTINATION ${SC3_PACKAGE_SBIN_DIR}
)

SC_INSTALL_INIT(httpmsgbus config/httpmsgbus.py)

IF(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/config/httpmsgbus.cfg)
        INSTALL(FILES ${CMAKE_CURRENT_SOURCE_DIR}/config/httpmsgbus.cfg
                DESTINATION ${SC3_PACKAGE_CONFIG_DIR})
ENDIF(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/config/httpmsgbus.cfg)

FILE(GLOB descs "${CMAKE_CURRENT_SOURCE_DIR}/descriptions/*.xml")
INSTALL(FILES ${descs} DESTINATION ${SC3_PACKAGE_APP_DESC_DIR})

