cmake_minimum_required(VERSION 3.22.1)
project(deeplog)

set(LIBDC ${CMAKE_SOURCE_DIR}/../../../../libdivecomputer)

# ── libdivecomputer ──────────────────────────────────────────────────────────
add_library(divecomputer STATIC
    ${LIBDC}/src/aes.c
    ${LIBDC}/src/array.c
    ${LIBDC}/src/atomics_cobalt.c
    ${LIBDC}/src/atomics_cobalt_parser.c
    ${LIBDC}/src/ble.c
    ${LIBDC}/src/bluetooth.c
    ${LIBDC}/src/buffer.c
    ${LIBDC}/src/checksum.c
    ${LIBDC}/src/citizen_aqualand.c
    ${LIBDC}/src/citizen_aqualand_parser.c
    ${LIBDC}/src/cochran_commander.c
    ${LIBDC}/src/cochran_commander_parser.c
    ${LIBDC}/src/common.c
    ${LIBDC}/src/context.c
    ${LIBDC}/src/cressi_edy.c
    ${LIBDC}/src/cressi_edy_parser.c
    ${LIBDC}/src/cressi_goa.c
    ${LIBDC}/src/cressi_goa_parser.c
    ${LIBDC}/src/cressi_leonardo.c
    ${LIBDC}/src/cressi_leonardo_parser.c
    ${LIBDC}/src/custom.c
    ${LIBDC}/src/datetime.c
    ${LIBDC}/src/deepblu_cosmiq.c
    ${LIBDC}/src/deepblu_cosmiq_parser.c
    ${LIBDC}/src/deepsix_excursion.c
    ${LIBDC}/src/deepsix_excursion_parser.c
    ${LIBDC}/src/descriptor.c
    ${LIBDC}/src/device.c
    ${LIBDC}/src/diverite_nitekq.c
    ${LIBDC}/src/diverite_nitekq_parser.c
    ${LIBDC}/src/divesoft_freedom.c
    ${LIBDC}/src/divesoft_freedom_parser.c
    ${LIBDC}/src/divesystem_idive.c
    ${LIBDC}/src/divesystem_idive_parser.c
    ${LIBDC}/src/halcyon_symbios.c
    ${LIBDC}/src/halcyon_symbios_parser.c
    ${LIBDC}/src/hdlc.c
    ${LIBDC}/src/hw_frog.c
    ${LIBDC}/src/hw_ostc3.c
    ${LIBDC}/src/hw_ostc.c
    ${LIBDC}/src/hw_ostc_parser.c
    ${LIBDC}/src/ihex.c
    ${LIBDC}/src/iostream.c
    ${LIBDC}/src/irda.c
    ${LIBDC}/src/iterator.c
    ${LIBDC}/src/liquivision_lynx.c
    ${LIBDC}/src/liquivision_lynx_parser.c
    ${LIBDC}/src/mares_common.c
    ${LIBDC}/src/mares_darwin.c
    ${LIBDC}/src/mares_darwin_parser.c
    ${LIBDC}/src/mares_iconhd.c
    ${LIBDC}/src/mares_iconhd_parser.c
    ${LIBDC}/src/mares_nemo.c
    ${LIBDC}/src/mares_nemo_parser.c
    ${LIBDC}/src/mares_puck.c
    ${LIBDC}/src/mclean_extreme.c
    ${LIBDC}/src/mclean_extreme_parser.c
    ${LIBDC}/src/oceanic_atom2.c
    ${LIBDC}/src/oceanic_atom2_parser.c
    ${LIBDC}/src/oceanic_common.c
    ${LIBDC}/src/oceanic_veo250.c
    ${LIBDC}/src/oceanic_veo250_parser.c
    ${LIBDC}/src/oceanic_vtpro.c
    ${LIBDC}/src/oceanic_vtpro_parser.c
    ${LIBDC}/src/oceans_s1.c
    ${LIBDC}/src/oceans_s1_common.c
    ${LIBDC}/src/oceans_s1_parser.c
    ${LIBDC}/src/packet.c
    ${LIBDC}/src/parser.c
    ${LIBDC}/src/pelagic_i330r.c
    ${LIBDC}/src/platform.c
    ${LIBDC}/src/rbstream.c
    ${LIBDC}/src/reefnet_sensus.c
    ${LIBDC}/src/reefnet_sensus_parser.c
    ${LIBDC}/src/reefnet_sensuspro.c
    ${LIBDC}/src/reefnet_sensuspro_parser.c
    ${LIBDC}/src/reefnet_sensusultra.c
    ${LIBDC}/src/reefnet_sensusultra_parser.c
    ${LIBDC}/src/ringbuffer.c
    ${LIBDC}/src/seac_screen.c
    ${LIBDC}/src/seac_screen_common.c
    ${LIBDC}/src/seac_screen_parser.c
    ${LIBDC}/src/serial_posix.c
    ${LIBDC}/src/shearwater_common.c
    ${LIBDC}/src/shearwater_petrel.c
    ${LIBDC}/src/shearwater_predator.c
    ${LIBDC}/src/shearwater_predator_parser.c
    ${LIBDC}/src/socket.c
    ${LIBDC}/src/sporasub_sp2.c
    ${LIBDC}/src/sporasub_sp2_parser.c
    ${LIBDC}/src/suunto_common2.c
    ${LIBDC}/src/suunto_common.c
    ${LIBDC}/src/suunto_d9.c
    ${LIBDC}/src/suunto_d9_parser.c
    ${LIBDC}/src/suunto_eon.c
    ${LIBDC}/src/suunto_eon_parser.c
    ${LIBDC}/src/suunto_eonsteel.c
    ${LIBDC}/src/suunto_eonsteel_parser.c
    ${LIBDC}/src/suunto_solution.c
    ${LIBDC}/src/suunto_solution_parser.c
    ${LIBDC}/src/suunto_vyper2.c
    ${LIBDC}/src/suunto_vyper.c
    ${LIBDC}/src/suunto_vyper_parser.c
    ${LIBDC}/src/tecdiving_divecomputereu.c
    ${LIBDC}/src/tecdiving_divecomputereu_parser.c
    ${LIBDC}/src/timer.c
    ${LIBDC}/src/usb.c
    ${LIBDC}/src/usbhid.c
    ${LIBDC}/src/uwatec_aladin.c
    ${LIBDC}/src/uwatec_memomouse.c
    ${LIBDC}/src/uwatec_memomouse_parser.c
    ${LIBDC}/src/uwatec_smart.c
    ${LIBDC}/src/uwatec_smart_parser.c
    ${LIBDC}/src/version.c
    ${LIBDC}/src/zeagle_n2ition3.c
)

target_include_directories(divecomputer PUBLIC ${LIBDC}/include)

target_compile_definitions(divecomputer PRIVATE
    ENABLE_LOGGING
    HAVE_VERSION_SUFFIX
    HAVE_PTHREAD_H
    HAVE_STRERROR_R
    HAVE_CLOCK_GETTIME
    HAVE_LOCALTIME_R
    HAVE_GMTIME_R
    HAVE_TIMEGM
    HAVE_STRUCT_TM_TM_GMTOFF
)

# ── JNI bridge ───────────────────────────────────────────────────────────────
add_library(deeplog SHARED bridge.cpp)

target_link_libraries(deeplog
    divecomputer
    android
    log
)
