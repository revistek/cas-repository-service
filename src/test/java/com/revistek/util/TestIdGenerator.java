package com.revistek.util;

// public class TestIdGenerator {
//  @Test
//  public void testUniqueId() throws Exception {
//    RedisCacheDao mockCacheDao = Mockito.mock(RedisCacheDao.class);
//    RandomNumberGenerator mockRandomNumberGenerator = Mockito.mock(RandomNumberGenerator.class);
//    UUID mockUuid = Mockito.mock(UUID.class);
//    Clock mockClock = Mockito.mock(Clock.class);
//    Mockito.when(mockRandomNumberGenerator.randomInt()).thenReturn(1);
//    Mockito.when(mockUuid.toString()).thenReturn("mockuuid");
//    Mockito.when(mockClock.millis()).thenReturn(19L);
//    Mockito.when(mockCacheDao.exists(ArgumentMatchers.anyString())).thenReturn(false);
//
//    IdGenerator idGenerator =
//        IdGenerator.newBuilder()
//            .cacheDao(mockCacheDao)
//            .uuid(mockUuid)
//            .clock(mockClock)
//            .randomGenerator(mockRandomNumberGenerator)
//            .delimiter(IdGenerator.DEFAULT_DELIMITER)
//            .build();
//    assertEquals("19_mockuuid_1", idGenerator.uniqueId());
//  }
//
//  @Test
//  public void testUniqueIdUseDefaults() throws Exception {
//    RedisCacheDao mockCacheDao = Mockito.mock(RedisCacheDao.class);
//    Mockito.when(mockCacheDao.exists(ArgumentMatchers.anyString())).thenReturn(false);
//
//    try (MockedStatic<UUID> mockUuid = Mockito.mockStatic(UUID.class)) {
//      UUID mockUuidObj = Mockito.mock(UUID.class);
//      Mockito.when(mockUuidObj.toString()).thenReturn("mockuuid");
//      mockUuid.when(() -> UUID.randomUUID()).thenReturn(mockUuidObj);
//
//      try (MockedStatic<Clock> mockClock = Mockito.mockStatic(Clock.class)) {
//        Clock mockClockObj = Mockito.mock(Clock.class);
//        Mockito.when(mockClockObj.millis()).thenReturn(19L);
//        mockClock.when(() -> Clock.systemUTC()).thenReturn(mockClockObj);
//
//        IdGenerator idGenerator = IdGenerator.newBuilder().cacheDao(mockCacheDao).build();
//        assertEquals("19_mockuuid_-1148943845", idGenerator.uniqueId());
//      }
//    }
//  }
//
//  @Test
//  public void testUniqueIdResolveDuplicate() throws Exception {
//    RedisCacheDao mockCacheDao = Mockito.mock(RedisCacheDao.class);
//    Mockito.when(mockCacheDao.exists(ArgumentMatchers.anyString()))
//        .thenReturn(true)
//        .thenReturn(false)
//        .thenReturn(false);
//
//    try (MockedStatic<UUID> mockUuid = Mockito.mockStatic(UUID.class)) {
//      UUID mockUuidObj = Mockito.mock(UUID.class);
//      Mockito.when(mockUuidObj.toString()).thenReturn("mockuuid");
//      mockUuid.when(() -> UUID.randomUUID()).thenReturn(mockUuidObj);
//
//      try (MockedStatic<Clock> mockClock = Mockito.mockStatic(Clock.class)) {
//        Clock mockClockObj = Mockito.mock(Clock.class);
//        Mockito.when(mockClockObj.millis()).thenReturn(19L);
//        mockClock.when(() -> Clock.systemUTC()).thenReturn(mockClockObj);
//
//        IdGenerator idGenerator = IdGenerator.newBuilder().cacheDao(mockCacheDao).build();
//        assertEquals("19_mockuuid_1107643289", idGenerator.uniqueId());
//      }
//    }
//  }
//
//  @Test
//  public void testUniqueIdCacheException() throws Exception {
//    RedisCacheDao mockCacheDao = Mockito.mock(RedisCacheDao.class);
//    Mockito.when(mockCacheDao.exists(ArgumentMatchers.anyString()))
//        .thenThrow(Exception.class)
//        .thenReturn(false);
//
//    try (MockedStatic<UUID> mockUuid = Mockito.mockStatic(UUID.class)) {
//      UUID mockUuidObj = Mockito.mock(UUID.class);
//      Mockito.when(mockUuidObj.toString()).thenReturn("mockuuid");
//      mockUuid.when(() -> UUID.randomUUID()).thenReturn(mockUuidObj);
//
//      try (MockedStatic<Clock> mockClock = Mockito.mockStatic(Clock.class)) {
//        Clock mockClockObj = Mockito.mock(Clock.class);
//        Mockito.when(mockClockObj.millis()).thenReturn(19L);
//        mockClock.when(() -> Clock.systemUTC()).thenReturn(mockClockObj);
//
//        IdGenerator idGenerator = IdGenerator.newBuilder().cacheDao(mockCacheDao).build();
//        assertEquals("19_mockuuid_-1148943845", idGenerator.uniqueId());
//      }
//    }
//  }
//
//  @Test
//  public void testRefreshAndGetUniqueId() throws Exception {
//    RedisCacheDao mockCacheDao = Mockito.mock(RedisCacheDao.class);
//    Mockito.when(mockCacheDao.exists(ArgumentMatchers.anyString())).thenReturn(false);
//
//    try (MockedStatic<UUID> mockUuid = Mockito.mockStatic(UUID.class)) {
//      UUID mockUuidObj = Mockito.mock(UUID.class);
//      Mockito.when(mockUuidObj.toString()).thenReturn("mockuuid");
//      mockUuid.when(() -> UUID.randomUUID()).thenReturn(mockUuidObj);
//
//      try (MockedStatic<Clock> mockClock = Mockito.mockStatic(Clock.class)) {
//        Clock mockClockObj = Mockito.mock(Clock.class);
//        Mockito.when(mockClockObj.millis()).thenReturn(19L);
//        mockClock.when(() -> Clock.systemUTC()).thenReturn(mockClockObj);
//
//        RandomNumberGenerator mockRandomNumberGeneratorObj =
//            Mockito.mock(RandomNumberGenerator.class);
//        Mockito.when(mockRandomNumberGeneratorObj.randomInt()).thenReturn(1);
//
//        IdGenerator idGenerator =
//            IdGenerator.newBuilder()
//                .cacheDao(mockCacheDao)
//                .uuid(mockUuidObj)
//                .clock(mockClockObj)
//                .randomGenerator(mockRandomNumberGeneratorObj)
//                .delimiter(IdGenerator.DEFAULT_DELIMITER)
//                .build();
//        assertEquals("19_mockuuid_1", idGenerator.uniqueId());
//        assertEquals("19_mockuuid_-1148943845", idGenerator.refreshAndGetUniqueId());
//        assertEquals("19_mockuuid_1107643289", idGenerator.uniqueId());
//      }
//    }
//  }
// }
