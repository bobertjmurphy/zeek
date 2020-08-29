
include(ZeekPlugin)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})

zeek_plugin_begin(Zeek Ascii_BatchWriter)
zeek_plugin_cc(Ascii_Batch.cc Plugin.cc)
zeek_plugin_bif(ascii_batch.bif)
zeek_plugin_end()
