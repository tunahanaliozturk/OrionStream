namespace Moongazing.OrionStream.Tests;

using Microsoft.Extensions.DependencyInjection;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class OrionStreamRegistrationTests
{
    [Fact]
    public void AddOrionStream_resolves_a_hub()
    {
        var services = new ServiceCollection();
        services.AddOrionStream();

        using var provider = services.BuildServiceProvider();
        Assert.IsType<SseHub>(provider.GetService<ISseHub>());
    }

    [Fact]
    public void AddOrionStream_honours_configured_options()
    {
        var services = new ServiceCollection();
        services.AddOrionStream(o => o.SubscriberCapacity = 16);

        using var provider = services.BuildServiceProvider();
        Assert.Equal(16, provider.GetRequiredService<StreamOptions>().SubscriberCapacity);
    }

    [Fact]
    public void AddOrionStream_rejects_invalid_options_eagerly()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            services.AddOrionStream(o => o.SubscriberCapacity = 0));
    }
}
