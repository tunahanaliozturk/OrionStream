namespace Moongazing.OrionStream.Redis.Tests;

/// <summary>
/// Validation conformance for <see cref="RedisReplayStoreOptions"/>. These are pure in-memory checks
/// (no Redis), so they do not take the container fixture.
/// </summary>
public sealed class RedisReplayStoreOptionsTests
{
    [Fact]
    public void Defaults_validate()
    {
        var options = new RedisReplayStoreOptions();
        options.Validate(); // does not throw
    }

    [Fact]
    public void Database_minus_one_is_the_documented_default_sentinel_and_validates()
    {
        var options = new RedisReplayStoreOptions { Database = -1 };
        options.Validate(); // -1 means "multiplexer default"
    }

    [Fact]
    public void Database_zero_and_positive_indices_validate()
    {
        new RedisReplayStoreOptions { Database = 0 }.Validate();
        new RedisReplayStoreOptions { Database = 5 }.Validate();
    }

    [Theory]
    [InlineData(-2)]
    [InlineData(-100)]
    [InlineData(int.MinValue)]
    public void Database_below_minus_one_is_rejected(int database)
    {
        var options = new RedisReplayStoreOptions { Database = database };
        var ex = Assert.Throws<ArgumentException>(options.Validate);
        Assert.Equal(nameof(RedisReplayStoreOptions.Database), ex.ParamName);
    }

    [Fact]
    public void Empty_key_prefix_is_rejected()
    {
        var options = new RedisReplayStoreOptions { KeyPrefix = string.Empty };
        var ex = Assert.Throws<ArgumentException>(options.Validate);
        Assert.Equal(nameof(RedisReplayStoreOptions.KeyPrefix), ex.ParamName);
    }

    [Fact]
    public void Non_positive_ttl_is_rejected()
    {
        var options = new RedisReplayStoreOptions { BacklogTimeToLive = TimeSpan.Zero };
        var ex = Assert.Throws<ArgumentException>(options.Validate);
        Assert.Equal(nameof(RedisReplayStoreOptions.BacklogTimeToLive), ex.ParamName);
    }
}
